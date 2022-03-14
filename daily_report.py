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
logger = logging.getLogger("daily_report")


class DailyReport():
    def __init__(self):
        self.main_path = Path('Y:\广告\【共用】媒介报告\阿语RoS\账户组\【KOH】账户组报告')
        self.source_filepath = None
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
        self.yesterday = self.now_date - timedelta(days=1)
        self.read_filepath = None
        self.write_filepath = None
        self.daily_text = None
        self.month_spend_target = None
        self.month_ndev_target = None
        self.act_price_target = None
        self.lost_price_target = None
        self.month_price_target = None
        self.month_ROI_target = None
        self.month_amount = None
        self.month_amount_complete = None
        self.plan_spend_complete = None
        self.plan_price_complete = None
        self.plan_ROI_complete = None
        self.spend_complete = None
        self.price_complete = None
        self.act_price_complete = None
        self.lost_price_complete = None
        self.ROI_complete = None
        self.cumulate_all_price = None
        self.cumulate_act_price = None
        self.cumulate_lost_price = None
        self.cumulate_spend = None
        self.cumulate_act_spend = None
        self.cumulate_lost_spend = None
        self.cumulate_all_ROI = None
        self.cumulate_act_ROI = None
        self.price_all = None
        self.price_act = None
        self.price_lost = None
        self.spend_all = None
        self.spend_all_pct = None
        self.num_dev_all = None
        self.num_dev_all_pct = None
        self.num_dev_act = None
        self.num_dev_lost = None
        self.num_dev_T1 = None
        self.num_dev_lost_T1 = None
        self.num_dev_act_T1 = None

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
    def read_excel(self, filepath, sheetName=0, header: int=0):
        df = pd.DataFrame()
        try:
            df = pd.read_excel(filepath, sheet_name = sheetName, header = header)
            logger.info(f'从[{filepath}]读取数据成功！')
        except Exception as e:
            logger.exception(f'从[{filepath}]读取数据失败，错误原因：{e}')
        return df

    # 读取数据源,若存在csv或者xlsx格式文件则读取并返回True,否则返回False
    def read_source(self):
        self.source_filepath = self.main_path.joinpath('数据源', 'data_daily.xlsx')
        if self.source_filepath.exists():
            self.df_source = self.read_excel(self.source_filepath, header=1)
        else:
            self.source_filepath = self.main_path.joinpath('数据源', 'data_daily.csv')
            if self.source_filepath.exists():
                self.df_source = self.read_csv(self.source_filepath)
            else:
                return False
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
        self.df_target = self.df_target.rename(columns={'渠道': 'channel_name','平台': 'platform',
                                              '设备类型': 'dev_type','月预算': 'month_spend',
                                              '月导量': 'month_ndev','月充值金额': 'month_price',
                                              '次留率': 'retent_rate'})
        # 筛选月流水目标
        self.df_target_amount = self.df_target[self.df_target['channel_name'] == '月流水'][['month_price']]
        self.df_target_amount = self.df_target_amount.rename(columns={'month_price': 'amount_target'}).reset_index(drop=True)
        # 筛选非月流水目标
        self.df_target = self.df_target[self.df_target['channel_name'] != '月流水']

    # 计算计划目标与完成度数据
    def cal_target_data(self, df_month_amount):
        # 计算总的目标值
        self.month_spend_target = self.df_target['month_spend'].sum()
        self.month_ndev_target = self.df_target['month_ndev'].sum()
        self.month_price_target = self.df_target['month_price'].sum()
        self.act_price_target = self.df_target[self.df_target['dev_type'] == '新增']['month_price'].sum()
        self.lost_price_target = self.df_target[self.df_target['dev_type'] == '召回']['month_price'].sum()
        self.month_ROI_target = self.month_price_target / self.month_spend_target
        # 将实际月流水和月流水目标合并
        df_month_amount = df_month_amount.join(self.df_target_amount)
        # 月流水金额和完成度
        self.month_amount = df_month_amount.loc[0, 'price']
        self.month_amount_complete = self.month_amount / self.df_target_amount.loc[0, 'amount_target']
        # 计算计划花费完成度、计划充值金额完成度、计划ROI完成度
        df_temp = self.df_spliced_pred_all.set_index('dates')
        self.plan_spend_complete = df_temp.loc[self.date_max_str, 'cum_spend'] / \
                                   df_temp.loc[self.thismonth_lastday.strftime('%Y-%m-%d'), 'cum_spend']
        self.plan_price_complete = df_temp.loc[self.date_max_str, 'cum_price'] / \
                                   df_temp.loc[self.thismonth_lastday.strftime('%Y-%m-%d'), 'cum_price']
        self.plan_ROI_complete = self.plan_price_complete / self.plan_spend_complete
        # 将月流水表和月总天数以及计划完成度合并
        df_month_amount = df_month_amount.join(pd.DataFrame(
            {'total_days': [self.totaldays_thismonth], 'plan_spend_complete': [self.plan_spend_complete],
             'plan_price_complete': [self.plan_price_complete], 'plan_ROI_complete': [self.plan_ROI_complete]}))
        return df_month_amount

    # 按category筛选数据
    def select_by_category(self):
        if not self.df_source.empty:
            # 筛选分日花费和充值总数据
            df_spend_rech_daily = self.df_source[self.df_source['category'] == 'daily'].drop(columns=['category'])
            self.date_max_str = df_spend_rech_daily['dates'].max()
            self.date_max = datetime.strptime(self.date_max_str, '%Y-%m-%d').date()
            self.thismonth_firstday = self.date_max.replace(day=1)
            self.future_firstday = self.date_max + timedelta(days=1)
            self.totaldays_thismonth = calendar._monthlen(self.date_max.year, self.date_max.month)
            self.future_days = self.totaldays_thismonth - self.date_max.day
            self.thismonth_lastday = self.date_max + timedelta(days=self.future_days)
            # 筛选月流水数据
            df_month_price = self.df_source[self.df_source['category'] == 'month_amount'][['dates','num_rech_dev','price']]
            df_month_price = df_month_price.rename(columns={'dates': 'now_days'})
            # 月流水数据添加日报最大日期
            df_month_price = pd.DataFrame({'dates': [self.date_max]}).join(df_month_price)
            # 是否有待训练数据，若无，则无需训练
            if self.df_source[self.df_source['category'] == 'train'].empty:
                self.train_or_not = False
                # 筛选本月分日花费和充值数据
                df_not_train = self.df_source[self.df_source['category'] == 'not_train'][[
                    'dev_type','dates','spending','price']]
                return {'daily': df_spend_rech_daily,
                        'month_amount': df_month_price,
                        'not_train': df_not_train}
            else:
                # 筛选待训练数据
                df_for_train = self.df_source[self.df_source['category'] == 'train'][
                    ['dev_type', 'dates', 'spending', 'price']]
                return {'daily': df_spend_rech_daily,
                        'month_amount': df_month_price,
                        'train': df_for_train}
        else:
            logger.info(f'数据源为空！')
            return False

    # 划分训练数据
    def split_train_data(self, df_for_train):
        price_act_train = df_for_train[df_for_train['dev_type'] == '新增'][['dates','price']].reset_index(drop=True)
        price_lost_train = df_for_train[df_for_train['dev_type'] == '召回'][['dates','price']].reset_index(drop=True)
        spend_train = df_for_train.groupby(['dates'], as_index=False)[['spending']].sum().reset_index(drop=True)
        return price_act_train, price_lost_train, spend_train

    # 利用ARIMA模型预测未来数据
    def ARIMA_forecast(self, data_category, train_data, future_days):
        if data_category == 'actPrice':
            stlf = STLForecast(train_data, ARIMA, model_kwargs=dict(order=(1,1,1)), seasonal=31, period=31)
            stlf_res = stlf.fit()
            forecast = stlf_res.forecast(future_days).reset_index(drop=True)
            # 将预测值中的负值置为0
            forecast = forecast.apply(lambda x: 0 if x < 0 else x)
            df_forecast = pd.DataFrame(
                {'dates': pd.Series(
                    pd.date_range(start=self.future_firstday.strftime('%Y-%m-%d'), periods=self.future_days)),
                 'act_pred_price': forecast})

        elif data_category == 'lostPrice':
            stlf = STLForecast(train_data, ARIMA, model_kwargs=dict(order=(1,1,1)), seasonal=31, period=31)
            stlf_res = stlf.fit()
            forecast = stlf_res.forecast(future_days).reset_index(drop=True)
            # 将预测值中的负值置为0
            forecast = forecast.apply(lambda x: 0 if x < 0 else x)
            df_forecast = pd.DataFrame(
                {'dates': pd.Series(
                    pd.date_range(start=self.future_firstday.strftime('%Y-%m-%d'), periods=self.future_days)),
                 'lost_pred_price': forecast})

        else:
            stlf = STLForecast(train_data, ARIMA, model_kwargs=dict(order=(0,1,0)), period=31)
            stlf_res = stlf.fit()
            forecast = stlf_res.forecast(future_days).reset_index(drop=True)
            # 将预测值中的负值置为0
            forecast = forecast.apply(lambda x: 0 if x < 0 else x)
            df_forecast = pd.DataFrame(
                {'dates': pd.Series(
                    pd.date_range(start=self.future_firstday.strftime('%Y-%m-%d'), periods=self.future_days)),
                 'pred_spend': forecast})

        return df_forecast

    # 将预测好的花费和充值数据与本月历史数据拼接到一起
    def splice_history_forecast(self, history_data, forecast_data, category):
        history_this_month = history_data[history_data['dates'] >= self.thismonth_firstday.strftime('%Y-%m-%d')]
        if category == 'actPrice':
            history_this_month = history_this_month.rename(columns={'price': 'act_pred_price'})
        elif category == 'lostPrice':
            history_this_month = history_this_month.rename(columns={'price': 'lost_pred_price'})
        else:
            history_this_month = history_this_month.rename(columns={'spending': 'pred_spend'})
        return history_this_month.append(forecast_data).reset_index(drop=True)

    # 计算累计数据
    def cal_cum_data(self, df_spend_rech_daily):
        # 按设备类型分组计算花费和充值
        df_dev_type_group = df_spend_rech_daily.groupby(['dev_type'])[['spending', 'price']].sum()
        # 计算累计总充值
        self.cumulate_all_price = df_dev_type_group.loc[:,'price'].sum()
        # 计算累计新增充值和召回充值
        self.cumulate_act_price = df_dev_type_group.loc['新增','price']
        self.cumulate_lost_price = df_dev_type_group.loc['召回','price']
        # 计算累计全部花费
        self.cumulate_spend = df_dev_type_group.loc[:,'spending'].sum()
        # 计算累计新增设备花费
        self.cumulate_act_spend = df_dev_type_group.loc['新增', 'spending']
        # 计算累计召回花费
        self.cumulate_lost_spend = df_dev_type_group.loc['召回', 'spending']
        # 计算累计项目ROI
        self.cumulate_all_ROI = self.cumulate_all_price / self.cumulate_spend
        # 计算累计新增设备ROI
        self.cumulate_act_ROI = self.cumulate_act_price / self.cumulate_act_spend
        # 按渠道名称、平台和设备类型计算花费和充值
        df_channel_group = df_spend_rech_daily.groupby([
            'channel_name','platform','dev_type'])[['spending','num_dev','num_rech_dev','price']].sum()
        # 合并计划目标和实际数据
        self.df_target = self.df_target.join(df_channel_group, on=['channel_name','platform','dev_type'])
        # 计算花费、充值金额和ROI实际完成度
        self.spend_complete = self.cumulate_spend / self.month_spend_target
        self.price_complete = self.cumulate_all_price / self.month_price_target
        self.ROI_complete = self.cumulate_all_ROI / self.month_ROI_target
        # 计算广告和自然充值金额完成度
        self.act_price_complete = self.cumulate_act_price / self.act_price_target
        self.lost_price_complete = self.cumulate_lost_price / self.lost_price_target
        return True

    # 计算昨日数据
    def cal_yesterday_data(self, df_spend_rech_daily):
        # 按日期分组计算花费、量级和充值
        df_group_daily = df_spend_rech_daily.groupby([
            'dates'])[['spending', 'num_dev', 'price']].sum()
        # 计算昨日花费、量级和充值
        self.spend_all = df_group_daily.loc[self.date_max_str, 'spending']
        self.num_dev_all = df_group_daily.loc[self.date_max_str, 'num_dev']
        self.price_all = df_group_daily.loc[self.date_max_str, 'price']
        # 计算昨日花费环比
        df_spend_pct_daily = df_group_daily.pct_change()
        self.spend_all_pct = df_spend_pct_daily.loc[self.date_max_str, 'spending']
        self.num_dev_all_pct = df_spend_pct_daily.loc[self.date_max_str, 'num_dev']
        # 按设备类型和日期分组计算花费和充值
        df_dev_type_group_daily = df_spend_rech_daily.groupby([
            'dev_type','dates'])[['spending','num_dev','price']].sum()
        # 计算昨日新增设备量级和充值、召回量级和充值
        self.num_dev_act = df_dev_type_group_daily.loc[('新增', self.date_max_str), 'num_dev']
        self.price_act = df_dev_type_group_daily.loc[('新增', self.date_max_str), 'price']
        self.num_dev_lost = df_dev_type_group_daily.loc[('召回', self.date_max_str), 'num_dev']
        self.price_lost = df_dev_type_group_daily.loc[('召回',self.date_max_str), 'price']
        # 将花费表按激活日期和地理区域分组
        df_region_group_daily = df_spend_rech_daily.groupby(['region','dates'])[['spending','num_dev','price']].sum()
        # 昨日中东T1量级
        self.num_dev_T1 = df_region_group_daily.loc[('中东T1',self.date_max_str), 'num_dev']
        # 昨日召回中东T1量级
        df_dev_region_group_daily = df_spend_rech_daily.groupby(['dev_type', 'region', 'dates'])[
            ['spending', 'num_dev', 'price']].sum()
        self.num_dev_lost_T1 = df_dev_region_group_daily.loc[('召回','中东T1',self.date_max_str), 'num_dev']
        # 昨日新增设备中东T1量级
        self.num_dev_act_T1 = self.num_dev_T1 - self.num_dev_lost_T1
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

    # 生成报告读取与存储文件名和路径
    def gen_filepath(self):
        report_path = self.main_path.joinpath('日报')
        before_2day = self.yesterday - timedelta(days=1)
        self.read_filepath = report_path.joinpath('【KOH】市场日报' + before_2day.strftime('%Y%m%d') + '.xlsx')
        before_nday = before_2day
        while not self.read_filepath.exists() and before_2day - before_nday < timedelta(days=15):
            before_nday -= timedelta(days=1)
            self.read_filepath = report_path.joinpath('【KOH】市场日报' + before_nday.strftime('%Y%m%d') + '.xlsx')

        if self.read_filepath.exists():
            self.write_filepath = report_path.joinpath('【KOH】市场日报' + self.yesterday.strftime('%Y%m%d') + '.xlsx')
            return True
        else:
            logger.info(f"请确保[{self.main_path}]存放有近期日报!")
            return False

    # 将多个dataframe列表输出到read_filepath,文件刷新并另存为write_filepath
    def save_to_excel(self, read_filepath, write_filepath, range_list, df_list):
        app = xw.App(visible=False, add_book=False)
        s0 = time.perf_counter()
        wb = app.books.open(read_filepath)
        try:
            sht_source = wb.sheets['数据源']
            sht_source.range('A4:BO2000').clear()
            for range_n, df in zip(range_list, df_list):
                sht_source.range(range_n).options(index=False).value = df
            s1 = time.perf_counter()
            logger.info(f'读取上次日报的时间为{s1 - s0: .2f}秒.')
            wb.api.RefreshAll()
            s2 = time.perf_counter()
            logger.info(f'刷新数据的时间为{s2 - s1: .2f}秒.')
            wb.save(write_filepath)
            logger.info(f'保存今日日报的时间为{time.perf_counter() - s2: .2f}秒.')
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
                logger.info(f'数据筛选完成！')
                self.gen_target_sheetname()
                self.read_target()
                s4 = time.perf_counter()
                logger.info(f'计划与目标读取成功！读取的时间为{s4 - s3: .2f}秒.')
                self.transfer_target_column()
                if self.train_or_not:
                    price_act_train, price_lost_train, spend_train = self.split_train_data(list_data['train'])
                    # 先预测花费
                    df_forecast = self.ARIMA_forecast('spend', spend_train['spending'], self.future_days)
                    # dates字段日期格式从'%Y-%m-%d %H:%M:%S'更改为'%Y-%m-%d'
                    df_forecast['dates'] = df_forecast['dates'].dt.strftime('%Y-%m-%d')
                    self.df_spliced_pred_all = self.splice_history_forecast(spend_train, df_forecast, 'spend')
                    # 预测新增和召回充值
                    list_category = ['actPrice', 'lostPrice']
                    for train_data, category in zip([price_act_train, price_lost_train], list_category):
                        df_forecast = self.ARIMA_forecast(category, train_data['price'], self.future_days)
                        # dates字段日期格式从'%Y-%m-%d %H:%M:%S'更改为'%Y-%m-%d'
                        df_forecast['dates'] = df_forecast['dates'].dt.strftime('%Y-%m-%d')
                        df_spliced_pred = self.splice_history_forecast(train_data, df_forecast, category)
                        self.df_spliced_pred_all = self.df_spliced_pred_all.join(df_spliced_pred.set_index('dates'),
                                                                                 on='dates')
                    s5 = time.perf_counter()
                    logger.info(f'训练历史数据并预测未来完成！训练预测的时间为{s5 - s4: .2f}秒.')
                else:
                    # 按设备类型筛选花费和充值
                    df_not_train = list_data['not_train']
                    self.df_spliced_pred_all = df_not_train[df_not_train['dev_type'] == '新增'][[
                        'dates', 'spending', 'price']].rename(
                        columns={'spending': 'pred_spend', 'price': 'act_pred_price'})
                    df_rech_lost = df_not_train[df_not_train['dev_type'] == '召回'][[
                        'dates', 'price']].rename(columns={'price': 'lost_pred_price'})
                    self.df_spliced_pred_all = self.df_spliced_pred_all.join(df_rech_lost.set_index('dates'),
                                                                             on='dates')
                # 添加一列作为充值金额总和
                self.df_spliced_pred_all['pred_price'] = self.df_spliced_pred_all.apply(
                    lambda x: x.act_pred_price + x.lost_pred_price, axis=1)
                # 添加累计值
                self.df_spliced_pred_all['cum_spend'] = self.df_spliced_pred_all['pred_spend'].cumsum()
                self.df_spliced_pred_all['cum_act_price'] = self.df_spliced_pred_all['act_pred_price'].cumsum()
                self.df_spliced_pred_all['cum_lost_price'] = self.df_spliced_pred_all['lost_pred_price'].cumsum()
                self.df_spliced_pred_all['cum_price'] = self.df_spliced_pred_all['pred_price'].cumsum()
                # 计算计划目标与完成度数据
                list_data['month_amount'] = self.cal_target_data(list_data['month_amount'])

                if self.cal_cum_data(list_data['daily']) and self.cal_yesterday_data(list_data['daily']):
                    logger.info(f'数据计算完成！')
                    self.daily_text = f"[{self.now_date}] 早安~打工人\n" \
                                      f"截至{self.date_max_str[5:]}，时间进度{self.date_max.day/self.totaldays_thismonth: .1%}，" \
                                      f"计划ROI完成度{self.plan_ROI_complete: .1%}，计划充值金额完成度{self.plan_price_complete: .1%}，" \
                                      f"计划花费完成度{self.plan_spend_complete: .1%};\n" \
                                      f"累计月流水${self.month_amount / 10000 if self.month_amount > 10000 else self.month_amount: .2f}{'W' if self.month_amount > 1000 else ''}，" \
                                      f"完成度{self.month_amount_complete: .1%};\n" \
                                      f"累计项目ROI{self.cumulate_all_ROI: .2%}，完成度{self.ROI_complete: .1%};\n" \
                                      f"累计充值${self.cumulate_all_price / 1000 if self.cumulate_all_price > 1000 else self.cumulate_all_price: .2f}{'k' if self.cumulate_all_price > 1000 else ''}，" \
                                      f"完成度{self.price_complete: .1%}；" \
                                      f"其中，新增充值${self.cumulate_act_price / 1000 if self.cumulate_act_price > 1000 else self.cumulate_act_price: .2f}{'k' if self.cumulate_act_price > 1000 else ''}，" \
                                      f"完成度{self.act_price_complete: .1%}；" \
                                      f"召回充值${self.cumulate_lost_price / 1000 if self.cumulate_lost_price > 1000 else self.cumulate_lost_price: .2f}{'k' if self.cumulate_lost_price > 1000 else ''}，" \
                                      f"完成度{self.lost_price_complete: .1%};\n" \
                                      f"累计花费${self.cumulate_spend / 1000 if self.cumulate_spend > 1000 else self.cumulate_spend: .2f}{'k' if self.cumulate_spend > 1000 else ''}，" \
                                      f"完成度{self.spend_complete: .1%};\n" \
                                      f"昨日充值${self.price_all: .1f}，其中新增设备充值${self.price_act: .1f}，" \
                                      f"召回充值${self.price_lost: .1f};\n" \
                                      f"昨日花费${self.spend_all / 1000: .2f}k，" \
                                      f"环比{'上升' if self.spend_all_pct > 0 else '下降'}{abs(self.spend_all_pct): .2%};\n" \
                                      f"昨日量级{self.num_dev_all / 1000: .1f}k，其中，新增设备量级{self.num_dev_act / 1000: .1f}k，" \
                                      f"召回设备占比{self.num_dev_lost / self.num_dev_all: .2%};\n" \
                                      f"昨日中东T1量级{self.num_dev_T1 / 1000: .1f}k，" \
                                      f"中东T1量级占比{self.num_dev_T1 / self.num_dev_all: .2%}，" \
                                      f"其中，新增设备中东T1量级{self.num_dev_act_T1 / 1000: .1f}k，" \
                                      f"召回中东T1量级{self.num_dev_lost_T1 / 1000: .1f}k.\n" \
                                      f"详情见 {self.main_path.joinpath('日报')}"
                    self.send_message()

                if self.gen_filepath():
                    self.save_to_excel(self.read_filepath, self.write_filepath,
                                       ['A1', 'A4', 'R4', 'AC4'],
                                       [list_data['month_amount'], list_data['daily'],
                                        self.df_spliced_pred_all, self.df_target])
        else:
            logger.info(f'[{self.source_filepath}]目录下没有文件！\n请重新检查文件存放路径！')
            return False


def main():
    s0 = time.perf_counter()
    daily_report = DailyReport()
    daily_report.run()
    logger.info(f'总用时{time.perf_counter() - s0: 0.2f}秒.')

if __name__ == '__main__':
    main()
