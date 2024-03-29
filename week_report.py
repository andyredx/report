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
        self.source_filepath = None
        self.target_filepath = self.main_path.joinpath('数据源', '投放计划与目标.xlsx')
        self.target_sheetname_thismonth = None
        self.target_sheetname_lastmonth = None
        self.df_source = None
        self.df_target_thismonth = None
        self.df_target_lastmonth = None
        self.df_target_weekly = pd.DataFrame()
        self.target_amount_thismonth = None
        self.target_amount_lastmonth = None
        self.train_or_not = True
        self.date_max = None
        self.date_max_str = None
        self.thismonth_firstday = None
        self.thismonth_lastday = None
        self.future_firstday = None
        self.now_date = date.today()
        self.totaldays_thismonth = None
        self.future_days = None
        self.lastmonth_lastday = None
        self.lastmonth_firstday = None
        self.totaldays_lastmonth = None
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
    def read_excel(self, filepath, sheetName=0):
        df = pd.DataFrame()
        try:
            df = pd.read_excel(filepath, sheet_name = sheetName)
            logger.info(f'从[{filepath}]的分表[{sheetName}]读取数据成功！')
        except Exception as e:
            logger.exception(f'从[{filepath}]的分表[{sheetName}]读取数据失败，错误原因：{e}')
        return df

    # 读取数据源,若存在csv或者xlsx格式文件则读取并返回True,否则返回False
    def read_source(self):
        self.source_filepath = self.main_path.joinpath('数据源', 'data_weekly.xlsx')
        if self.source_filepath.exists():
            self.df_source = self.read_excel(self.source_filepath)
        else:
            self.source_filepath = self.main_path.joinpath('数据源', 'data_weekly.csv')
            if self.source_filepath.exists():
                self.df_source = self.read_csv(self.source_filepath)
            else:
                return False
        return True

    # 读取投放计划与目标
    def read_target(self):
        self.df_target_thismonth = self.read_excel(self.target_filepath, self.target_sheetname_thismonth)
        self.df_target_lastmonth = self.read_excel(self.target_filepath, self.target_sheetname_lastmonth)
        return True

    # 生成目标分表名称
    def gen_target_sheetname(self):
        self.target_sheetname_thismonth = str(self.date_max.month) + '月'
        self.target_sheetname_lastmonth = str(self.lastmonth_firstday.month) + '月'

    # 清洗目标分表的列名
    def transfer_target_column(self, df_target):
        df_target = df_target.rename(columns={'渠道': 'channel_name','地理区域': 'region',
                                              '月预算': 'month_spend','月导量': 'month_ndev',
                                              '日均预算': 'spend_daily', '日均导量': 'ndev_daily',
                                              '月ROI': 'month_ROI','周ROI': 'week_ROI',
                                              '月充值金额': 'month_price','次留率': 'retent_rate'})
        df_target['week_ad_price'] = df_target.apply(lambda x: x.spend_daily * 7 * x.week_ROI, axis=1)
        df_target = df_target.drop(columns=['week_ROI','成本(CPI)','预算占比','量级占比','充值占比'])
        # 筛选月流水目标
        df_target_amount = df_target[df_target['channel_name'] == '月流水'].reset_index(drop=True)
        target_amount = df_target_amount.loc[0, 'month_price']
        # 筛选非月流水目标
        df_target = df_target[df_target['channel_name'] != '月流水']
        return df_target, target_amount

    # 按category筛选数据
    def select_by_category(self):
        if not self.df_source.empty:
            # 筛选月同期总数据
            df_now_month = self.df_source[self.df_source['category'] == 'now_month'].drop(columns=['category'])
            df_now_month = df_now_month.rename(columns={'dates': 'month_category'})
            # 筛选月度分日花费和充值数据
            df_ROI_daily = self.df_source[(self.df_source['category'] == 'this_month') |
                                            (self.df_source['category'] == 'last_month') |
                                            (self.df_source['category'] == 'last_year_month')][[
                'category','dates','channel_type','spending','num_dev','price']]
            df_ROI_daily = df_ROI_daily.rename(columns={'category': 'month_category'})
            # 计算本月各个日期
            self.date_max_str = df_ROI_daily['dates'].max()
            self.date_max = datetime.strptime(self.date_max_str, '%Y-%m-%d').date()
            self.thismonth_firstday = self.date_max.replace(day=1)
            self.future_firstday = self.date_max + timedelta(days=1)
            self.totaldays_thismonth = calendar._monthlen(self.date_max.year, self.date_max.month)
            self.future_days = self.totaldays_thismonth - self.date_max.day
            self.thismonth_lastday = self.date_max + timedelta(days=self.future_days)
            # 计算上月各个日期
            self.lastmonth_lastday = self.thismonth_firstday - timedelta(days=1)
            self.lastmonth_firstday = self.lastmonth_lastday.replace(day=1)
            self.totaldays_lastmonth = calendar._monthlen(self.lastmonth_firstday.year, self.lastmonth_firstday.month)
            # 筛选周度总数据
            df_weekly = self.df_source[self.df_source['category'] == 'weekly'].drop(columns=['category'])
            df_weekly = df_weekly.rename(columns={'dates': 'act_weeks'})
            # 筛选月流水数据
            df_month_price = self.df_source[self.df_source['category'] == 'month_amount'][['dates','channel_type','price']]
            df_month_price = df_month_price[df_month_price['channel_type'] != 'last_year_month']
            df_month_price = df_month_price.rename(columns={'dates': 'now_days',
                                                            'channel_type': 'month_category',
                                                            'price': 'month_price'}).reset_index(drop=True)
            # 月流水数据添加月度总天数
            df_month_price = pd.DataFrame({'total_days': [self.totaldays_lastmonth, self.totaldays_thismonth]}).join(df_month_price)
            df_month_price = df_month_price[['month_category','total_days','now_days','month_price']]
            # 是否有待训练数据，若无，则无需训练
            if self.df_source[self.df_source['category'] == 'price'].empty:
                self.train_or_not = False
                return {'daily': df_ROI_daily,
                        'weekly': df_weekly,
                        'monthly': df_now_month,
                        'month_amount': df_month_price}
            else:
                # 筛选待训练充值数据
                df_price_for_learn = self.df_source[self.df_source['category'] == 'price'][['channel_type','dates','price']]
                # 筛选待训练花费数据
                df_spend_for_learn = self.df_source[self.df_source['category'] == 'spending'][['dates','spending']]
                return {'daily': df_ROI_daily,
                        'weekly': df_weekly,
                        'monthly': df_now_month,
                        'month_amount': df_month_price,
                        'price': df_price_for_learn,
                        'spending': df_spend_for_learn}
        else:
            logger.info(f'数据源为空！')
            return False

    # 指定训练数据与未来预测天数
    def split_train_data(self, df_price_for_learn, df_spend_for_learn):
        price_advert_train = df_price_for_learn[df_price_for_learn['channel_type'] == '广告'][['dates','price']].reset_index(drop=True)
        price_organic_train = df_price_for_learn[df_price_for_learn['channel_type'] == '自然'][['dates','price']].reset_index(drop=True)
        spend_train = df_spend_for_learn[['dates','spending']].reset_index(drop=True)
        return price_advert_train, price_organic_train, spend_train

    # 利用ARIMA模型预测未来数据
    def ARIMA_forecast(self, data_category, train_data, future_days):
        if data_category == 'adPrice':
            stlf = STLForecast(train_data, ARIMA, model_kwargs=dict(seasonal_order=(1, 0, 1, 31)), seasonal=31, period=31)
            stlf_res = stlf.fit()
            forecast = stlf_res.forecast(future_days).reset_index(drop=True)
            # 将预测值中的负值置为0
            forecast = forecast.apply(lambda x: 0 if x < 0 else x)
            df_forecast = pd.DataFrame(
                {'dates': pd.Series(
                    pd.date_range(start=self.future_firstday.strftime('%Y-%m-%d'), periods=self.future_days)),
                 'ad_pred_price': forecast})

        elif data_category == 'orPrice':
            stlf = STLForecast(train_data, ARIMA, model_kwargs=dict(order=(1,0,1)), seasonal=31, period=31)
            stlf_res = stlf.fit()
            forecast = stlf_res.forecast(future_days).reset_index(drop=True)
            # 将预测值中的负值置为0
            forecast = forecast.apply(lambda x: 0 if x < 0 else x)
            df_forecast = pd.DataFrame(
                {'dates': pd.Series(
                    pd.date_range(start=self.future_firstday.strftime('%Y-%m-%d'), periods=self.future_days)),
                 'or_pred_price': forecast})

        else:
            stlf = STLForecast(train_data, ARIMA, model_kwargs=dict(order=(0, 1, 0)), period=31)
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
        if category == 'adPrice':
            history_this_month = history_this_month.rename(columns={'price': 'ad_pred_price'})
        elif category == 'orPrice':
            history_this_month = history_this_month.rename(columns={'price': 'or_pred_price'})
        else:
            history_this_month = history_this_month.rename(columns={'spending': 'pred_spend'})
        return history_this_month.append(forecast_data).reset_index(drop=True)

    # 根据周起始日期生成周目标
    def gen_target_week(self):
        nweeks = 0
        list_names = ['本周', '前一周', '前两周', '前三周', '前四周']
        self.df_target_weekly = pd.DataFrame(
            columns=['week_category', 'week_spend', 'week_ndev', 'week_or_ndev', 'week_ad_price'])
        while nweeks < 5:
            last_n_Sunday = self.date_max - timedelta(days=1 + nweeks * 7 + self.date_max.weekday())
            gap_days = last_n_Sunday - self.thismonth_firstday
            # 周天数大部分在上个月
            if gap_days.days < 0 and abs(gap_days.days) > 3:
                self.df_target_weekly = self.df_target_weekly.append({
                    'week_category': list_names[nweeks],
                    'week_spend': 7 * self.df_target_lastmonth.loc[:, 'spend_daily'].sum(),
                    'week_ndev': 7 * self.df_target_lastmonth.loc[:, 'ndev_daily'].sum(),
                    'week_or_ndev': 7 * self.df_target_lastmonth[self.df_target_lastmonth['channel_name'] == '自然渠道'
                                                                  ].loc[:, 'ndev_daily'].sum(),
                    'week_ad_price': self.df_target_lastmonth.loc[:, 'week_ad_price'].sum()}, ignore_index=True)
            else:
                self.df_target_weekly = self.df_target_weekly.append({
                    'week_category': list_names[nweeks],
                    'week_spend': 7 * self.df_target_thismonth.loc[:, 'spend_daily'].sum(),
                    'week_ndev': 7 * self.df_target_thismonth.loc[:, 'ndev_daily'].sum(),
                    'week_or_ndev': 7 * self.df_target_thismonth[self.df_target_thismonth['channel_name'] == '自然渠道'
                                                                  ].loc[:, 'ndev_daily'].sum(),
                    'week_ad_price': self.df_target_thismonth.loc[:, 'week_ad_price'].sum()}, ignore_index=True)
            nweeks += 1

    # 生成报告读取与存储文件名和路径
    def gen_filepath(self):
        report_filepath = self.main_path.joinpath('周报')
        nweeks = 1
        last_n_Sunday = self.date_max - timedelta(days=1 + nweeks*7 + self.date_max.weekday())
        last_n_Saturday = last_n_Sunday + timedelta(days=6)
        filename = '【KOH】市场周报' + last_n_Sunday.strftime('%Y%m%d') + '-' + \
                   last_n_Saturday.strftime('%Y%m%d') + '.xlsx'
        self.read_filepath = report_filepath.joinpath(filename)
        while not self.read_filepath.exists() and nweeks < 5:
            nweeks += 1
            last_n_Sunday = self.date_max - timedelta(days=1 + nweeks * 7 + self.date_max.weekday())
            last_n_Saturday = last_n_Sunday + timedelta(days=6)
            filename = '【KOH】市场周报' + last_n_Sunday.strftime('%Y%m%d') + '-' + \
                       last_n_Saturday.strftime('%Y%m%d') + '.xlsx'
            self.read_filepath = report_filepath.joinpath(filename)

        if self.read_filepath.exists():
            last_Sunday = self.date_max - timedelta(days=1 + self.date_max.weekday())
            last_Saturday = last_Sunday + timedelta(days=6)
            filename = '【KOH】市场周报' + last_Sunday.strftime('%Y%m%d') + '-' + \
                       last_Saturday.strftime('%Y%m%d') + '.xlsx'
            self.write_filepath = report_filepath.joinpath(filename)
            return True
        else:
            logger.info(f"请确保[{report_filepath}]存放有近期周报!")
            return False

    # 将实际月流水和月流水目标合并,计算计划目标完成度数据
    def cal_target_data(self, df_month_amount):
        df_month_amount = df_month_amount.join(pd.DataFrame(
            {'amount_target': [self.target_amount_lastmonth, self.target_amount_thismonth]}))
        # 计算计划花费完成度、计划充值金额完成度、计划ROI完成度
        df_temp = self.df_spliced_pred_all.set_index('dates')
        plan_spend_complete = df_temp.loc[self.date_max_str, 'cum_spend'] / \
                                   df_temp.loc[self.thismonth_lastday.strftime('%Y-%m-%d'), 'cum_spend']
        plan_price_complete = df_temp.loc[self.date_max_str, 'cum_price'] / \
                                   df_temp.loc[self.thismonth_lastday.strftime('%Y-%m-%d'), 'cum_price']
        plan_ROI_complete = plan_price_complete / plan_spend_complete
        # 将月流水表和月总天数以及计划完成度合并
        df_month_amount = df_month_amount.join(pd.DataFrame(
            {'plan_spend_complete': [None, plan_spend_complete], 'plan_price_complete': [None, plan_price_complete],
             'plan_ROI_complete': [None, plan_ROI_complete]}))
        return df_month_amount

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
            sht_source.range('A6:BO2000').clear()
            for range_n, df in zip(range_list, df_list):
                sht_source.range(range_n).options(index=False).value = df
            s1 = time.perf_counter()
            logger.info(f'读取上次周报的时间为{s1 - s0: .2f}秒.')
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
                if self.gen_filepath():
                    self.gen_target_sheetname()
                    self.read_target()
                    s4 = time.perf_counter()
                    logger.info(f'计划与目标读取成功！读取的时间为{s4 - s3: .2f}秒.')
                    self.df_target_thismonth, self.target_amount_thismonth = self.transfer_target_column(self.df_target_thismonth)
                    self.df_target_lastmonth, self.target_amount_lastmonth = self.transfer_target_column(self.df_target_lastmonth)
                    self.gen_target_week()
                    if self.train_or_not:
                        price_advert_train, price_organic_train, spend_train = self.split_train_data(list_data['price'],
                                                                                                     list_data['spending'])
                        # 先预测花费
                        df_forecast = self.ARIMA_forecast('spend', spend_train['spending'], self.future_days)
                        # dates字段日期格式从'%Y-%m-%d %H:%M:%S'更改为'%Y-%m-%d'
                        df_forecast['dates'] = df_forecast['dates'].dt.strftime('%Y-%m-%d')
                        self.df_spliced_pred_all = self.splice_history_forecast(spend_train, df_forecast, 'spend')
                        # 预测广告和自然充值
                        list_category = ['adPrice', 'orPrice']
                        for train_data, category in zip([price_advert_train, price_organic_train], list_category):
                            df_forecast = self.ARIMA_forecast(category, train_data['price'], self.future_days)
                            # dates字段日期格式从'%Y-%m-%d %H:%M:%S'更改为'%Y-%m-%d'
                            df_forecast['dates'] = df_forecast['dates'].dt.strftime('%Y-%m-%d')
                            df_spliced_pred = self.splice_history_forecast(train_data, df_forecast, category)
                            self.df_spliced_pred_all = self.df_spliced_pred_all.join(df_spliced_pred.set_index('dates'),
                                                                                     on='dates')
                        s5 = time.perf_counter()
                        logger.info(f'训练历史数据并预测未来完成！训练预测的时间为{s5 - s4: .2f}秒.')
                    else:
                        # 按渠道类型和日期分组计算花费和充值
                        df_channel_type_daily = list_data['daily']
                        df_channel_type_daily = df_channel_type_daily[df_channel_type_daily['month_category'] == 'this_month'].drop(columns=['month_category'])
                        self.df_spliced_pred_all = df_channel_type_daily[df_channel_type_daily['channel_type'] == '广告'][[
                        'dates','spending','price']].rename(columns={'spending': 'pred_spend','price': 'ad_pred_price'})
                        df_rech_or = df_channel_type_daily[df_channel_type_daily['channel_type'] == '自然'][[
                        'dates','price']].rename(columns={'price': 'or_pred_price'})
                        self.df_spliced_pred_all = self.df_spliced_pred_all.join(df_rech_or.set_index('dates'),
                                                                                 on='dates')
                    # 添加一列作为充值金额总和
                    self.df_spliced_pred_all['pred_price'] = self.df_spliced_pred_all.apply(
                        lambda x: x.ad_pred_price + x.or_pred_price, axis=1)
                    # 添加累计值
                    self.df_spliced_pred_all['cum_spend'] = self.df_spliced_pred_all['pred_spend'].cumsum()
                    self.df_spliced_pred_all['cum_ad_price'] = self.df_spliced_pred_all['ad_pred_price'].cumsum()
                    self.df_spliced_pred_all['cum_or_price'] = self.df_spliced_pred_all['or_pred_price'].cumsum()
                    self.df_spliced_pred_all['cum_price'] = self.df_spliced_pred_all['pred_price'].cumsum()
                    # 将实际月流水和月流水目标合并,计算计划目标完成度数据
                    list_data['month_amount'] = self.cal_target_data(list_data['month_amount'])

                    self.save_to_excel(self.read_filepath, self.write_filepath,
                                       ['A1','A6','P6','X6','AI6','AU6','BB6'],
                                       [list_data['month_amount'], list_data['monthly'],
                                        list_data['daily'], self.df_spliced_pred_all,
                                        self.df_target_thismonth, self.df_target_weekly,
                                        list_data['weekly']])
                    self.week_text = f"[{date.today()}] 【KOH】市场周报已更新至：{self.write_filepath}"
                    self.send_message()
        else:
            logger.info(f'[{self.source_filepath}]目录下没有文件！\n请重新检查文件存放路径！')
            return False


def main():
    s0 = time.perf_counter()
    week_report = WeekReport()
    week_report.run()
    logger.info(f'总用时{time.perf_counter() - s0: .2f}秒.')


if __name__ == '__main__':
    main()
