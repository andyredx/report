#!/usr/bin/env python
# encoding=utf-8

import xlwings as xw
import pandas as pd
import requests,json
import time
from datetime import datetime,date,timedelta
from pathlib import Path
import logging
import sys

logging.basicConfig(
    format = "%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level = logging.INFO,
    datefmt = "%H:%M:%S",
    stream = sys.stderr
)
logger = logging.getLogger("week_report")


class WeekReport():
    def __init__(self):
        self.main_path = Path('Y:\广告\【共用】媒介报告\阿语RoS\账户组\【KOH】账户组周报')
        self.recharge_filepath = self.main_path.joinpath('数据源', 'week_recharge.csv')
        self.spend_filepath = self.main_path.joinpath('数据源', 'week_spend.csv')
        self.today = date.today()
        self.read_filepath = None
        self.write_filepath = None
        self.df_mon_week_day = pd.DataFrame()
        self.df_ROI_month = pd.DataFrame()
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

    # 读取数据源
    def read_source(self):
        self.df_mon_week_day = self.read_csv(self.spend_filepath)
        self.df_ROI_month = self.read_csv(self.recharge_filepath)
        return True

    # 生成周报读取与存储文件名和路径
    def gen_filepath(self):
        last_3_Sunday = self.today - timedelta(days=15 + self.today.weekday())
        last_2_Saturday = self.today - timedelta(days=9 + self.today.weekday())
        filename = '【KOH】市场周报' + last_3_Sunday.strftime('%Y%m%d') + '-' + \
                   last_2_Saturday.strftime('%Y%m%d') + '.xlsx'
        self.read_filepath = self.main_path.joinpath(filename)
        last_2_Sunday = self.today - timedelta(days=8 + self.today.weekday())
        last_Saturday = self.today - timedelta(days=2 + self.today.weekday())
        filename = '【KOH】市场周报' + last_2_Sunday.strftime('%Y%m%d') + '-' + \
                   last_Saturday.strftime('%Y%m%d') + '.xlsx'
        self.write_filepath = self.main_path.joinpath(filename)
        return True

    # 按flag筛选数据
    def select_by_flag(self):
        if not self.df_mon_week_day.empty:
            # 筛选本月数据
            df_this_month = self.df_mon_week_day[self.df_mon_week_day['flag'] == 'this_month'].drop(columns=['flag'])
            df_this_month.insert(0, '时间', df_this_month.pop('时间').apply(lambda x: datetime.strptime(x, '%Y-%m-%d')))
            # 筛选上月数据
            df_last_month = self.df_mon_week_day[self.df_mon_week_day['flag'] == 'last_month'].drop(columns=['flag'])
            df_last_month.insert(0, '时间', df_last_month.pop('时间').apply(lambda x: datetime.strptime(x, '%Y-%m-%d')))
            # 筛选分周数据
            df_week = self.df_mon_week_day[self.df_mon_week_day['flag'] == 'week'].drop(columns=['flag'])
            # 近五周数据
            list_weeks = sorted(set(df_week['时间']), reverse=True)
            list_week_names = ['本周', '前一周', '前两周', '前三周', '前四周']
            dic_week_names = dict(zip(list_weeks, list_week_names))
            df_week.insert(0, '周时间', df_week.apply(lambda x: dic_week_names[x['时间']], axis=1))
            df_week = df_week.drop(columns=['时间'])
            # 筛选上月同期总充值和本月总充值、目前天数和本月总天数
            df_price_days = self.df_mon_week_day[self.df_mon_week_day['flag'] == 'day']
            df_price_days = df_price_days[['充值金额', '时间']]
            is_MW_empty = False
        else:
            is_MW_empty = True
            logger.info(f'月同期和分周数据为空！')
        if not self.df_ROI_month.empty:
            self.df_ROI_month.insert(1, '时间', self.df_ROI_month.pop('时间').apply(lambda x: datetime.strptime(x, '%Y-%m-%d')))
            # 去年同月
            df_ROI_last_year = self.df_ROI_month[self.df_ROI_month['flag'] == 'last_year_month'].drop(columns=['flag'])
            # 上月
            df_ROI_last_month = self.df_ROI_month[self.df_ROI_month['flag'] == 'last_month'].drop(columns=['flag'])
            # 本月
            df_ROI_this_month = self.df_ROI_month[self.df_ROI_month['flag'] == 'this_month'].drop(columns=['flag'])
            is_ROI_empty = False
        else:
            is_ROI_empty = True
            logger.info(f'分月ROI数据为空！')
        # 如果月同期、分周数据和分月ROI数据都不为空
        if not (is_MW_empty | is_ROI_empty):
            return [df_price_days, df_this_month, df_last_month, df_week, df_ROI_this_month,
                            df_ROI_last_month, df_ROI_last_year]

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
            self.gen_filepath()
            list_data = self.select_by_flag()
            if list_data:
                s3 = time.perf_counter()
                logger.info(f'数据筛选完成！筛选的时间为{s3 - s2: .2f}秒.')
                self.save_to_excel(self.read_filepath, self.write_filepath,
                                   ['B1', 'A8', 'O8', 'AC8', 'AR8', 'AY8', 'BF8'],
                                   list_data)
                self.week_text = f"[{self.today}] 【KOH】市场周报已更新至：{self.write_filepath}"
                self.send_message()


def main():
    s0 = time.perf_counter()
    week_report = WeekReport()
    week_report.run()
    logger.info(f'总用时{time.perf_counter() - s0: .2f}秒.')


if __name__ == '__main__':
    main()
