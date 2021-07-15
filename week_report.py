#!/usr/bin/env python
# encoding=utf-8

import json
import logging
import sys
import time, calendar
from datetime import date, datetime, timedelta
from pathlib import Path
from statsmodels.tsa.forecasting.stl import STLForecast
from statsmodels.tsa.arima.model import ARIMA

import pandas as pd
import requests
import xlwings as xw

logging.basicConfig(
    format = "%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level = logging.INFO,
    datefmt = "%H:%M:%S",
    stream = sys.stderr
)
logger = logging.getLogger("week_report")


class WeekReport():
    def __init__(self):
        self.main_path = Path('Y:\广告\【共用】媒介报告\阿语RoS\账户组\【KOH】账户组报告')
        self.source_filepath = self.main_path.joinpath('数据源', 'data_weekly.csv')
        self.target_filepath = self.main_path.joinpath('数据源', '投放计划与目标.xlsx')
        self.target_sheetname = None
        self.df_source = None
        self.df_target = None
        self.df_target_amount = None
        self.train_or_not = True
        self.date_max = None
        self.date_max_str = None
        self.thismonth_firstday = None
        self.thismonth_lastday = None
        self.future_firstday = None
        self.now_date = date.today()
        self.totaldays_thismonth = None
        self.future_days = None
        self.df_spliced_pred_all = None
        self.read_filepath = None
        self.write_filepath = None
        self.week_text = None

    # 读取csv文件，返回dataframe
    def read_csv(self, filepath):
        df = pd.DataFrame()
        try:
            df = pd.read_csv(filepath)
            logger.info(f'从[{filepath}]读取数据成功！')
        except Exception as e:
            logger.exception(f'从[{filepath}]读取数据失败，错误原因：{e}')
        return df

    # 读取excel文件，返回dataframe
    def read_excel(self, filepath, sheetName):
        df = pd.DataFrame()
        try:
            df = pd.read_excel(filepath, sheet_name = sheetName)
            logger.info(f'从[{filepath}]读取数据成功！')
        except Exception as e:
            logger.exception(f'从[{filepath}]读取数据失败，错误原因：{e}')
        return df

    # 读取数据源
    def read_source(self):
        self.df_source = self.read_csv(self.source_filepath)
        return True

    # 读取投放计划与目标
    def read_target(self):
        self.df_target = self.read_excel(self.target_filepath, self.target_sheetname)
        return True

    # 生成目标分表名称
    def gen_target_sheetname(self):
        self.target_sheetname = str(self.date_max.month) + '月'

    # 清洗目标分表的列名
    def transfer_target_column(self):
        self.df_target = self.df_target.rename(columns={'渠道': 'channel_name','受众': 'orientate',
                                                        '月预算': 'month_spend','月导量': 'month_ndev',
                                                        '月ROI': 'month_ROI','周ROI': 'week_ROI',
                                                        '月充值金额': 'month_price','次留率': 'retent_rate'})
        self.df_target = self.df_target.drop(columns=['日均预算','日均导量','成本(CPI)','预算占比','量级占比','充值占比'])
        # 筛选月流水目标
        self.df_target_amount = self.df_target[self.df_target['channel_name'] == '月流水'].drop(columns=[
                'channel_name','orientate','month_spend','month_ndev','month_ROI','week_ROI','retent_rate'])
        self.df_target_amount = self.df_target_amount.rename(columns={'month_price': 'amount_target'}).reset_index(drop=True)
        # 筛选非月流水目标
        self.df_target = self.df_target[self.df_target['channel_name'] != '月流水']

    # 按category筛选数据
    def select_by_category(self):
        if not self.df_source.empty:
            # 筛选月同期总数据
            df_now_month = self.df_source[self.df_source['category'] == 'now_month'].drop(columns=['category'])
            # 筛选月度分日花费和充值数据
            df_ROI_monthly = self.df_source[(self.df_source['category'] == 'this_month') |
                                            (self.df_source['category'] == 'last_month') |
                                            (self.df_source['category'] == 'last_year_month')][[
                'category','dates','channel_type','spending','price']]
            # 计算本月各个日期
            self.date_max_str = df_ROI_monthly['dates'].max()
            self.date_max = datetime.strptime(self.date_max_str, '%Y-%m-%d').date()
            self.thismonth_firstday = self.date_max.replace(day=1)
            self.future_firstday = self.date_max + timedelta(days=1)
            self.totaldays_thismonth = calendar._monthlen(self.date_max.year, self.date_max.month)
            self.future_days = self.totaldays_thismonth - self.date_max.day
            self.thismonth_lastday = self.date_max + timedelta(days=self.future_days)
            # 筛选周度总数据
            df_weekly = self.df_source[self.df_source['category'] == 'weekly'].drop(columns=['category'])
            df_weekly = df_weekly.rename(columns={'dates': 'act_weeks'})
            # # 筛选月流水数据
            # df_month_price = self.df_source[self.df_source['category'] == 'month_amount'][['dates','num_rech_dev','price']]
            # df_month_price = df_month_price.rename(columns={'dates': 'now_days'})
            # # 月流水数据添加日报最大日期
            # df_month_price = pd.DataFrame({'dates': [self.date_max]}).join(df_month_price)
            # # 是否有待训练数据，若无，则无需训练
            # if self.df_source[self.df_source['category'] == 'price'].empty:
            #     self.train_or_not = False
            #     # 筛选本月分日花费和充值数据
            #     df_channel_type_group_daily = self.df_source[self.df_source['category'] == 'spending'][[
            #         'channel_type','dates','spending','price']]
            #     return {'daily': df_spend_rech_daily,
            #             'weekly': df_spend_rech_weekly,
            #             'month_amount': df_month_price,
            #             'channel_daily': df_channel_type_group_daily}
            # else:
            #     # 筛选待训练充值数据
            #     df_price_for_learn = self.df_source[self.df_source['category'] == 'price'][['channel_type','dates','price']]
            #     # 筛选待训练花费数据
            #     df_spend_for_learn = self.df_source[self.df_source['category'] == 'spending'][['dates','spending']]
            #     return {'daily': df_spend_rech_daily,
            #             'weekly': df_spend_rech_weekly,
            #             'month_amount': df_month_price,
            #             'price': df_price_for_learn,
            #             'spending': df_spend_for_learn}
        else:
            logger.info(f'数据源为空！')
            return False

    # 生成周报读取与存储文件名和路径
    def gen_filepath(self):
        if self.today.weekday() == 6:
            self.today = date.today() + timedelta(days=1)
        last_3_Sunday = self.today - timedelta(days=15 + self.today.weekday())
        last_2_Saturday = self.today - timedelta(days=9 + self.today.weekday())
        filename = '【KOH】市场周报' + last_3_Sunday.strftime('%Y%m%d') + '-' + \
                   last_2_Saturday.strftime('%Y%m%d') + '.xlsx'
        self.read_filepath = self.main_path.joinpath(filename)
        # 判断读取文件路径是否存在，若无，则再往前一周
        if not self.read_filepath.exists():
            last_4_Sunday = self.today - timedelta(days=22 + self.today.weekday())
            last_3_Saturday = self.today - timedelta(days=16 + self.today.weekday())
            filename = '【KOH】市场周报' + last_4_Sunday.strftime('%Y%m%d') + '-' + \
                       last_3_Saturday.strftime('%Y%m%d') + '.xlsx'
            self.read_filepath = self.main_path.joinpath(filename)
        last_2_Sunday = self.today - timedelta(days=8 + self.today.weekday())
        last_Saturday = self.today - timedelta(days=2 + self.today.weekday())
        filename = '【KOH】市场周报' + last_2_Sunday.strftime('%Y%m%d') + '-' + \
                   last_Saturday.strftime('%Y%m%d') + '.xlsx'
        self.write_filepath = self.main_path.joinpath(filename)
        return True

    # 通过钉钉机器人发送信息
    def send_message(self):
        headers = {'Content-Type': 'application/json'}
        webhook = 'https://oapi.dingtalk.com/robot/send?access_token=0b77d70a9e88cd080b299b5bb7c8b83687a1fee89e6f3b3e75ed0dfacaf06410'
        data = {
            "msgtype": "text",
            "text": {"content": self.week_text},
            "isAtAll": True}
        try:
            response = requests.post(webhook, data=json.dumps(data), headers=headers, timeout=8)
        except Exception:
            logging.info("失去响应！")
            return False
        if response.status_code == 200:
            dict_res = response.json()
            if dict_res['errcode'] == 310000:
                logging.info(f"消息发送失败！失败原因：{dict_res['errmsg']}")
                return False
            elif dict_res['errcode'] == 0:
                logging.info("消息发送成功！")
                return True
            return dict_res
        elif response.status_code == 404:
            logging.info("该页面不存在！")
            return False
        else:
            logging.info("消息发送失败！response.status_code错误")
            return False

    # 将多个dataframe列表保存到excel,文件刷新并另存为write_filepath
    def save_to_excel(self, read_filepath, write_filepath, range_list, df_list):
        app = xw.App(visible=False, add_book=False)
        s0 = time.perf_counter()
        wb = app.books.open(read_filepath)
        try:
            sht_source = wb.sheets['数据源']
            sht_source.range('A8:BO2000').clear()
            for range_n, df in zip(range_list, df_list):
                sht_source.range(range_n).options(index=False).value = df
            sht_target = wb.sheets['进度及目标']
            sht_target.range('C30:J34').value = sht_target.range('C31:J35').value
            sht_target.range('C57:I64').value = sht_target.range('C66:I73').value
            s1 = time.perf_counter()
            logger.info(f'读取上周周报的时间为{s1 - s0: .2f}秒.')
            wb.api.RefreshAll()
            s2 = time.perf_counter()
            logger.info(f'刷新数据的时间为{s2 - s1: .2f}秒.')
            wb.save(write_filepath)
            logger.info(f'保存本周周报的时间为{time.perf_counter() - s2: .2f}秒.')
        except Exception as e:
            logger.exception(f'操作excel失败，错误原因：{e}')
        finally:
            wb.close()
            app.quit()


    def run(self):
        s1 = time.perf_counter()
        if self.read_source():
            s2 = time.perf_counter()
            logger.info(f'数据源读取成功！读取的时间为{s2 - s1: .2f}秒.')
            list_data = self.select_by_category()
            if list_data:
                s3 = time.perf_counter()
                logger.info(f'数据筛选完成！筛选的时间为{s3 - s2: .2f}秒.')
                # self.save_to_excel(self.read_filepath, self.write_filepath,
                #                    ['B1', 'A8', 'O8', 'AC8', 'AR8', 'AY8', 'BF8'],
                #                    list_data)
                # self.week_text = f"[{date.today()}] 【KOH】市场周报已更新至：{self.write_filepath}"
                # self.send_message()


def main():
    s0 = time.perf_counter()
    week_report = WeekReport()
    week_report.run()
    logger.info(f'总用时{time.perf_counter() - s0: .2f}秒.')


if __name__ == '__main__':
    main()
