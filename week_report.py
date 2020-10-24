#!/usr/bin/env python
# encoding=utf-8

import xlwings as xw
import pandas as pd
import time
from datetime import datetime
import logging
import sys

logging.basicConfig(
    format = "%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level = logging.INFO,
    datefmt = "%H:%M:%S",
    stream = sys.stderr
)
logger = logging.getLogger("week_report")


# 读取csv文件，返回dataframe
def read_csv(filepath):
    df = pd.DataFrame()
    try:
        df = pd.read_csv(filepath)
        logger.info(f'从[{filepath}]读取数据成功！')
    except Exception as e:
        logger.exception(f'从[{filepath}]读取数据失败，错误原因：{e}')
    return df


# 将多个dataframe列表保存到excel
def save_to_excel(filepath, range_list, df_list):
    app = xw.App(visible=False, add_book=False)
    wb = app.books.open(filepath)
    try:
        sht_source = wb.sheets['数据源']
        sht_source.range('A8:BO2000').clear()
        for range_n, df in zip(range_list, df_list):
            sht_source.range(range_n).options(index = False).value = df
        sht_target = wb.sheets['进度及目标']
        sht_target.range('C30:J34').value = sht_target.range('C31:J35').value
        sht_target.range('C57:I64').value = sht_target.range('C66:I73').value
        wb.api.RefreshAll()
    except Exception as e:
        logger.exception(f'保存失败，错误原因：{e}')
    finally:
        wb.save()
        wb.close()
        app.quit()


def main():
    read_filepath1 = r'Y:\广告\【共用】媒介报告\阿语RoS\账户组\【KOH】账户组优化日志\沈浮\小组周报\week_data1.csv'
    read_filepath2 = r'Y:\广告\【共用】媒介报告\阿语RoS\账户组\【KOH】账户组优化日志\沈浮\小组周报\week_data2.csv'
    # channel_obj_path = r'D:\龙腾简合\小组周报\【KOH】7月数据复盘和8月目标制定-月初修改版V2.xlsx'
    write_filepath = r'Y:\广告\【共用】媒介报告\阿语RoS\账户组\【KOH】账户组优化日志\沈浮\小组周报\【KOH】市场周报20201004-20201010.xlsx'
    s1 = time.perf_counter()
    # 读取月同期和分周数据，以及上月同期总充值和本月总充值、目前天数和本月总天数
    df_mon_week_day = read_csv(read_filepath1)
    # 读取分月花费和充值数据
    df_ROI_month = read_csv(read_filepath2)
    s2 = time.perf_counter()
    logger.info(f'读取的时间为 {s2 - s1:0.2f} 秒.')
    # 获取当前时间
    now_time = pd.Timestamp.now()
    now_year = now_time.year
    now_month = now_time.month
    now_day = now_time.day
    df_mon_begin_obj = pd.DataFrame()
    # # 月初修改本月目标
    # if now_day <= 7:
    #     mon_begin_obj_path = r'Y:\广告\【共用】媒介报告\0 项目通用文件和项目目标\回收目标\【市场部】各项目月目标(new).xlsx'
    #     sheet_name = f'{now_year}年{now_month}月'
    #     df_mon_begin_obj = read_excel(mon_begin_obj_path, sheet_name, 'F3')
    # df_channel_obj = read_excel(channel_obj_path, str(now_month)+'月目标制定', 'F20')
    if not df_mon_week_day.empty:
        # 筛选月同期数据
        df_now_mon = df_mon_week_day[df_mon_week_day['flag'] == 'month'].drop(columns=['flag'])
        df_now_mon.insert(0, '时间', df_now_mon.pop('时间').apply(lambda x: datetime.strptime(x, '%Y-%m-%d')))
        # 本月同期
        start_date = pd.Timestamp(now_year, now_month, 1)
        df_this_month = df_now_mon[df_now_mon['时间'] >= start_date]
        # 上月同期
        start_date = pd.Timestamp(now_year, now_month - 1, 1)
        end_date = pd.Timestamp(now_year, now_month, 1)
        df_last_month = df_now_mon[(df_now_mon['时间'] >= start_date) & (df_now_mon['时间'] < end_date)]
        # 筛选分周数据
        df_week = df_mon_week_day[df_mon_week_day['flag'] == 'week'].drop(columns=['flag'])
        # 近五周数据
        list_weeks = sorted(set(df_week['时间']), reverse=True)
        list_week_names = ['本周', '前一周', '前两周', '前三周', '前四周']
        dic_week_names = dict(zip(list_weeks, list_week_names))
        df_week.insert(0, '周时间', df_week.apply(lambda x: dic_week_names[x['时间']], axis=1))
        df_week = df_week.drop(columns=['时间'])
        # 筛选上月同期总充值和本月总充值、目前天数和本月总天数
        df_price_days = df_mon_week_day[df_mon_week_day['flag'] == 'day']
        df_price_days = df_price_days[['充值金额', '时间']]
        is_MW_empty = False
    else:
        is_MW_empty = True
        logger.info(f'月同期和分周数据为空！')
    if not df_ROI_month.empty:
        df_ROI_month.insert(1, '时间', df_ROI_month.pop('时间').apply(lambda x: datetime.strptime(x, '%Y-%m-%d')))
        # 去年同月
        start_date = pd.Timestamp(now_year - 1, now_month, 1)
        end_date = pd.Timestamp(now_year - 1, now_month + 1, 1)
        df_ROI_last_year = df_ROI_month[(df_ROI_month['时间'] >= start_date) &
                                     (df_ROI_month['时间'] < end_date)]
        # 上月
        start_date = pd.Timestamp(now_year, now_month - 1, 1)
        end_date = pd.Timestamp(now_year, now_month, 1)
        df_ROI_last_month = df_ROI_month[(df_ROI_month['时间'] >= start_date) &
                                     (df_ROI_month['时间'] < end_date)]
        # 本月
        start_date = pd.Timestamp(now_year, now_month, 1)
        end_date = pd.Timestamp(now_year, now_month + 1, 1)
        df_ROI_this_month = df_ROI_month[(df_ROI_month['时间'] >= start_date) &
                                     (df_ROI_month['时间'] < end_date)]
        is_ROI_empty = False
    else:
        is_ROI_empty = True
        logger.info(f'分月ROI数据为空！')
    # 如果月同期、分周数据和分月ROI数据都不为空
    if not (is_MW_empty | is_ROI_empty):
        s3 = time.perf_counter()
        logger.info(f'数据筛选完成！筛选的时间为 {s3 - s2:0.2f} 秒.')
        save_to_excel(write_filepath,
                      ['B1', 'A8', 'O8', 'AC8', 'AR8', 'AY8', 'BF8'],
                      [df_price_days, df_this_month, df_last_month, df_week, df_ROI_this_month,
                       df_ROI_last_month, df_ROI_last_year])
        s4 = time.perf_counter()
        logger.info(f'数据保存成功！保存的时间为 {s4 - s3:0.2f} 秒.')


if __name__ == '__main__':
    main()
