#!/usr/bin/env python
# encoding=utf-8

import xlwings as xw
import pandas as pd
from pathlib import Path
import time
from datetime import date, timedelta
import random
import logging, sys
import requests, json


logging.basicConfig(
    format = "%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level = logging.INFO,
    datefmt = "%H:%M:%S",
    stream = sys.stderr
)
logger = logging.getLogger("daily_report")


class DailyReport():
    def __init__(self):
        self.main_path = Path('Y:\广告\【共用】媒介报告\阿语RoS\账户组\【KOH】账户组日报')
        self.recharge_filepath = self.main_path.joinpath('数据源','daily_recharge.csv')
        self.spend_filepath = self.main_path.joinpath('数据源','daily_spend.csv')
        self.now_date = date.today()
        self.yesterday = self.now_date - timedelta(days=1)
        self.before_2day = self.yesterday - timedelta(days=1)
        self.report_path_old = self.main_path.joinpath('0-报告存放','KOH账户组执行进度追踪报告-' +
                                 self.before_2day.strftime('%Y%m%d') + '.xlsx')
        self.report_path_new = self.main_path.joinpath('0-报告存放', 'KOH账户组执行进度追踪报告-' +
                                                       self.yesterday.strftime('%Y%m%d') + '.xlsx')
        self.daily_text = None
        self.df_recharge = pd.DataFrame()
        self.df_spend = pd.DataFrame()
        self.date_max = None
        self.cumulate_all_price = None
        self.cumulate_spon_price = None
        self.cumulate_ad_price = None
        self.cumulate_spend = None
        self.cumulate_ad_ROI = None
        self.cumulate_all_ROI = None
        self.price_all = None
        self.price_spon = None
        self.price_ad = None
        self.spend_all = None
        self.spend_all_pct = None
        self.num_dev_all = None
        self.num_dev_all_pct = None
        self.num_dev_spon = None
        self.num_dev_core = None

    # 读取csv文件，返回dataframe
    def read_csv(self, filepath):
        df = pd.DataFrame()
        try:
            df = pd.read_csv(filepath)
            logger.info(f'从[{filepath}]读取数据成功！')
        except Exception as e:
            logger.exception(f'从[{filepath}]读取数据失败，错误原因：{e}')
        return df

    # 计算累计数据
    def cal_cum_data(self):
        # 将充值表按渠道名称分组
        df_channel_price = self.df_recharge.groupby(['channelname'])['price'].sum()
        # 计算累计总充值
        self.cumulate_all_price = df_channel_price.sum()
        # 计算累计自然充值和广告充值
        self.cumulate_spon_price = df_channel_price.loc['自然渠道']
        self.cumulate_ad_price = self.cumulate_all_price - self.cumulate_spon_price
        # 计算累计广告花费
        self.cumulate_spend = self.df_spend['spending'].sum()
        # 计算累计项目ROI
        self.cumulate_all_ROI = self.cumulate_all_price / self.cumulate_spend
        # 计算累计广告ROI
        self.cumulate_ad_ROI = self.cumulate_ad_price / self.cumulate_spend
        return True

    # 计算昨日数据
    def cal_yesterday_data(self):
        # 将充值表按充值日期和渠道名称分组
        df_rech_channel_daily = self.df_recharge.groupby(['recharge_date', 'channelname'])['price'].sum()
        # 计算昨日总充值和自然、广告充值
        self.price_all = df_rech_channel_daily.loc[self.date_max].sum()
        self.price_spon = df_rech_channel_daily.loc[(self.date_max, '自然渠道')]
        self.price_ad = self.price_all - self.price_spon
        # 将花费表按激活日期分组
        df_spend_daily = self.df_spend.groupby(['dates'])[['spending', 'num_devices']].sum()
        # 计算总花费分日环比
        df_spend_pct_daily = df_spend_daily.pct_change()
        # 昨日总花费和环比
        self.spend_all = df_spend_daily.loc[(self.date_max, 'spending')]
        self.spend_all_pct = df_spend_pct_daily.loc[self.date_max, 'spending']
        # 昨日总量级和环比
        self.num_dev_all = df_spend_daily.loc[(self.date_max, 'num_devices')]
        self.num_dev_all_pct = df_spend_pct_daily.loc[self.date_max, 'num_devices']
        # 将花费表按激活日期和渠道名称分组
        df_spend_channel_daily = self.df_spend.groupby(['dates', 'channelname']).sum()
        # 计算昨日自然量级
        self.num_dev_spon = df_spend_channel_daily.loc[(self.date_max, '自然渠道'), 'num_devices']
        # 将花费表按激活日期和受众分组
        df_spend_region = self.df_spend.groupby(['dates', 'orientate']).sum()
        # 昨日核心量级
        self.num_dev_core = df_spend_region.loc[(self.date_max, '核心'), 'num_devices']
        return True

    # 读取数据源
    def read_source(self):
        self.df_recharge = self.read_csv(self.recharge_filepath)
        self.df_spend = self.read_csv(self.spend_filepath)
        self.date_max = self.df_spend['dates'].max()
        return True

    # 通过钉钉机器人发送信息
    def send_message(self):
        headers = {'Content-Type': 'application/json'}
        webhook = 'https://oapi.dingtalk.com/robot/send?access_token=0b77d70a9e88cd080b299b5bb7c8b83687a1fee89e6f3b3e75ed0dfacaf06410'
        data = {
            "msgtype": "text",
            "text": {"content": self.daily_text},
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

    # 判断report_path_old是否存放有近期日报，若无，则查找最近一期的日报文件路径
    def is_old_path_exist(self):
        before_nday = self.before_2day
        while not self.report_path_old.exists() and self.before_2day - before_nday < timedelta(days=15):
            before_nday -= timedelta(days=1)
            self.report_path_old = self.main_path.joinpath('0-报告存放', 'KOH账户组执行进度追踪报告-' +
                                                            before_nday.strftime('%Y%m%d') + '.xlsx')
        if self.report_path_old.exists():
            return True
        else:
            logger.info(f"请确保[{self.main_path.joinpath('0-报告存放')}]存放有近期日报!")
            return False

    # 将filepath_old文件刷新并另存为filepath_new
    def save_excel(self, filepath_old, filepath_new):
        app = xw.App(visible=False, add_book=False)
        try:
            wb = app.books.open(filepath_old)
            s1 = time.perf_counter()
            wb.api.RefreshAll()
            s2 = time.perf_counter()
            logger.info(f'刷新数据的时间为{s2 - s1: .2f}秒.')
            wb.save(filepath_new)
            logger.info(f'保存为新文件的时间为{time.perf_counter() - s2: .2f}秒.')
        except Exception as e:
            logger.exception(f'打开excel失败，错误原因：{e}')
        finally:
            wb.close()
            app.quit()

    def run(self):
        if self.read_source():
            if self.cal_cum_data() and self.cal_yesterday_data():
                logger.info(f'数据计算完成！')
                self.daily_text = f"[{self.now_date}] 早安~打工人\n" \
                                  f"截至{self.date_max[5:]}，\n" \
                                  f"累计充值${self.cumulate_all_price / 1000 if self.cumulate_all_price > 1000 else self.cumulate_all_price: .2f}{'k' if self.cumulate_all_price > 1000 else ''}，" \
                                  f"其中自然充值${self.cumulate_spon_price / 1000 if self.cumulate_spon_price > 1000 else self.cumulate_spon_price: .2f}{'k' if self.cumulate_spon_price > 1000 else ''}，" \
                                  f"广告充值${self.cumulate_ad_price / 1000 if self.cumulate_ad_price > 1000 else self.cumulate_ad_price: .2f}{'k' if self.cumulate_ad_price > 1000 else ''};\n" \
                                  f"累计项目ROI{self.cumulate_all_ROI: .2%}，累计广告ROI{self.cumulate_ad_ROI: .2%};\n" \
                                  f"昨日充值${self.price_all: .1f}，其中自然充值${self.price_spon: .1f}，" \
                                  f"广告充值${self.price_ad: .1f};\n" \
                                  f"昨日花费${self.spend_all / 1000: .2f}k，" \
                                  f"环比{'上升' if self.spend_all_pct > 0 else '下降'}{abs(self.spend_all_pct): .2%};\n" \
                                  f"昨日量级{self.num_dev_all / 1000: .1f}k，自然量级{self.num_dev_spon / 1000: .1f}k，" \
                                  f"自然占比{self.num_dev_spon / self.num_dev_all: .2%};\n" \
                                  f"昨日核心量级{self.num_dev_core / 1000: .1f}k，" \
                                  f"核心量级占比{self.num_dev_core / self.num_dev_all: .2%}."
                self.send_message()
            if self.is_old_path_exist():
                self.save_excel(self.report_path_old, self.report_path_new)


def main():
    s0 = time.perf_counter()
    daily_report = DailyReport()
    daily_report.run()
    logger.info(f'总用时{time.perf_counter() - s0: 0.2f}秒.')

if __name__ == '__main__':
    main()
