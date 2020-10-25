#!/usr/bin/env python
# encoding=utf-8

import xlwings as xw
import pandas as pd
import time
import logging
import sys

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
    stream=sys.stderr
)
logger = logging.getLogger("month_report")


# 读取csv文件，返回dataframe
def read_csv(filepath):
    df = pd.DataFrame()
    try:
        df = pd.read_csv(filepath)
        logger.info(f'从[{filepath}]读取数据成功！')
    except Exception as e:
        logger.exception(f'从[{filepath}]读取数据失败，错误原因：{e}')
    return df

def save_to_excel(filepath, range_list, df_list):
    app = xw.App(visible=False, add_book=False)
    wb = app.books.open(filepath)
    try:
        sht = wb.sheets['数据源']
        sht.range('A8:BO2000').clear()
        for range_n, df in zip(range_list, df_list):
            sht.range(range_n).options(index = False).value = df
        wb.api.RefreshAll()
    except Exception as e:
        logger.exception(f'保存失败，错误原因：{e}')
    finally:
        wb.save()
        wb.close()
        app.quit()


def main():
    read_filepath1 = r'Y:\广告\【共用】媒介报告\阿语RoS\账户组\【KOH】账户组优化日志\沈浮\小组周报\month_data1.csv'
    read_filepath2 = r'Y:\广告\【共用】媒介报告\阿语RoS\账户组\【KOH】账户组优化日志\沈浮\小组周报\month_data2.csv'
    write_filepath = r'Y:\广告\【共用】媒介报告\阿语RoS\账户组\【KOH】账户组月报\广告3组【KOH】项目月报2020年10月整月.xlsx'
    s1 = time.perf_counter()
    # 读取月同期和月总充值数据
    df_mon_day = read_csv(read_filepath1)
    # 读取分月花费和充值数据
    df_ROI_month = read_csv(read_filepath2)
    s2 = time.perf_counter()
    logger.info(f'读取的时间为 {s2 - s1:0.2f} 秒.')
    if not df_mon_day.empty:
        # 筛选本月数据
        df_this_month = df_mon_day[df_mon_day['flag'] == 'this_month'].drop(columns=['flag'])
        # 筛选上月数据
        df_last_month = df_mon_day[df_mon_day['flag'] == 'last_month'].drop(columns=['flag'])
        # 筛选上月同期总充值和本月总充值、目前天数
        df_price_days = df_mon_day[df_mon_day['flag'] == 'day']
        df_price_days = df_price_days[['充值金额', '时间']]
        is_MD_empty = False
    else:
        is_MD_empty = True
        logger.info(f'月同期数据为空！')
    if not df_ROI_month.empty:
        # 去年同月
        df_ROI_last_year = df_ROI_month[df_ROI_month['flag'] == 'last_year_month'].drop(columns=['flag'])
        # 上月
        df_ROI_last_month = df_ROI_month[df_ROI_month['flag'] == 'last_month'].drop(columns=['flag'])
        # 本月
        df_ROI_this_month = df_ROI_month[df_ROI_month['flag'] == 'this_month'].drop(columns=['flag'])
        is_ROI_empty = False
    else:
        is_ROI_empty = True
        logger.info(f'分月ROI数据为空！')
    # 如果月同期、分周数据和分月ROI数据都不为空
    if not (is_MD_empty | is_ROI_empty):
        s3 = time.perf_counter()
        logger.info(f'数据筛选完成！筛选的时间为 {s3 - s2:0.2f} 秒.')
        save_to_excel(write_filepath,
                      ['B1', 'A8', 'O8', 'AR8', 'AY8', 'BF8'],
                      [df_price_days, df_this_month, df_last_month, df_ROI_this_month,
                       df_ROI_last_month, df_ROI_last_year])
        s4 = time.perf_counter()
        logger.info(f'数据保存成功！保存的时间为 {s4 - s3:0.2f} 秒.')


if __name__ == '__main__':
    main()
