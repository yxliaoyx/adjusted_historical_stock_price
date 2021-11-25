import os
import wget
import time
import datetime

import pandas as pd

from read_date import get_XRXD_data
from read_date import get_stock_data
from read_date import get_listing_date
from read_date import get_reduction_data
from read_date import get_listed_stockNo


def download_listed_company():
    for industry_code in range(1, 35):
        url = f'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&industry_code={industry_code:02}'

        try:
            time.sleep(3)
            dfs = pd.read_html(url, header=0, encoding='cp950')

            dir_path = os.path.join('data', 'listed_company')
            os.makedirs(dir_path, exist_ok=True)
            dfs[0].to_csv(os.path.join(dir_path, f'{industry_code}.csv'), index=False, encoding='cp950')

            print('downloaded', industry_code)


        except Exception as e:
            print(e)
            continue


def download_stock_csv(stockNo, start_date, end_date):
    listing_date_dict = get_listing_date()

    for pd_period in pd.period_range(start=start_date, end=end_date, freq='M'):
        if stockNo in listing_date_dict:
            listing_date = pd.to_datetime(listing_date_dict[stockNo])
            if pd_period.year < listing_date.year:
                continue
            if pd_period.year == listing_date.year and pd_period.month < listing_date.month:
                continue

        date = f'{pd_period.year}{pd_period.month:02}01'
        url = f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=csv&date={date}&stockNo={stockNo}'
        filename = os.path.join('data', 'stock_csv', stockNo, f'{stockNo}_{date}.csv')

        if not os.path.exists(filename):
            os.makedirs(os.path.join('data', 'stock_csv', stockNo), exist_ok=True)

            try:
                time.sleep(3)
                wget.download(url, filename)
                print('downloaded', filename)
            except Exception as e:
                with open(os.path.join('data', 'download.log'), 'a') as out_file:
                    out_file.write(f'{url}\n{e}\n')
                continue

        if os.path.getsize(filename) <= 2:
            os.remove(filename)


def update_stock_csv(stockNo, start_date, end_date):
    for pd_period in pd.period_range(start=start_date, end=end_date, freq='M'):
        date = f'{pd_period.year}{pd_period.month:02}01'
        url = f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=csv&date={date}&stockNo={stockNo}'
        filename = os.path.join('data', 'stock_csv', stockNo, f'{stockNo}_{date}.csv')

        if os.path.exists(filename):
            os.remove(filename)

        os.makedirs(os.path.join('data', 'stock_csv', stockNo), exist_ok=True)

        try:
            time.sleep(3)
            wget.download(url, filename)
            print('downloaded', filename)
        except Exception as e:
            with open(os.path.join('data', 'update.log'), 'a') as out_file:
                out_file.write(f'{url}\n{e}\n')
            continue

        if os.path.getsize(filename) <= 2:
            os.remove(filename)


def download_XRXD_csv():
    endDate = datetime.date.today().strftime('%Y%m%d')
    url = f'https://www.twse.com.tw/exchangeReport/TWT49U?response=csv&strDate=20030505&endDate={endDate}'

    dir_path = os.path.join('data', 'XRXD_csv')
    filename = os.path.join(dir_path, 'XRXD.csv')

    if os.path.exists(filename):
        os.remove(filename)

    os.makedirs(dir_path, exist_ok=True)
    wget.download(url, filename)
    print('downloaded', filename)


def download_reduction_csv():
    endDate = datetime.date.today().strftime('%Y%m%d')
    url = f'https://www.twse.com.tw/exchangeReport/TWTAUU?response=csv&strDate=20110101&endDate={endDate}'

    dir_path = os.path.join('data', 'reduction_csv')
    filename = os.path.join(dir_path, 'reduction.csv')

    if os.path.exists(filename):
        os.remove(filename)

    os.makedirs(dir_path, exist_ok=True)
    wget.download(url, filename)
    print('downloaded', filename)


def adjust_stock_data(stockNo, start_date, end_date):
    dir_name = os.path.join('data', 'adjusted_stock_data')
    os.makedirs(dir_name, exist_ok=True)

    XRXD_df = get_XRXD_data()
    reduction_df = get_reduction_data()

    stock_data = get_stock_data(stockNo, start_date, end_date)

    adjusted_stock_data = stock_data.copy()

    for _, XRXD_row in XRXD_df[XRXD_df['股票代號'] == stockNo].iterrows():
        XRXD_adj = XRXD_row['減除股利參考價'] / XRXD_row['除權息前收盤價']

        date_filter = adjusted_stock_data['日期'] < XRXD_row['資料日期']

        for column_name in ['開盤價', '最高價', '最低價', '收盤價', '漲跌價差']:
            adjusted_stock_data.loc[date_filter, column_name] = adjusted_stock_data[column_name] * XRXD_adj

    for _, reduction_row in reduction_df[reduction_df['股票代號'] == stockNo].iterrows():
        reduction_adj = reduction_row['恢復買賣參考價'] / reduction_row['停止買賣前收盤價格']

        date_filter = adjusted_stock_data['日期'] < reduction_row['恢復買賣日期']

        for column_name in ['開盤價', '最高價', '最低價', '收盤價', '漲跌價差']:
            adjusted_stock_data.loc[date_filter, column_name] = adjusted_stock_data[column_name] * reduction_adj

    adjusted_stock_data.to_csv(os.path.join(dir_name, f'{stockNo}.csv'), index=False, encoding='cp950')


if __name__ == '__main__':
    time_daily_update = time.time()

    # download_listed_company()
    #
    # download_XRXD_csv()
    # download_reduction_csv()
    #
    # industry_code_list = range(35)
    # data_start_date = '201001'
    # data_end_date = '202111'
    # for stockNo in get_listed_stockNo(industry_code_list):
    #     print(stockNo)
    #     download_stock_csv(stockNo, data_start_date, data_end_date)
    #
    # industry_code_list = range(35)
    # data_start_date = '202111'
    # data_end_date = '202111'
    # for stockNo in get_listed_stockNo(industry_code_list):
    #     print(stockNo)
    #     update_stock_csv(stockNo, data_start_date, data_end_date)

    industry_code_list = range(35)
    data_start_date = '20100101'
    data_end_date = '20211130'
    time_preprocess_stock_data = time.time()
    for stockNo in get_listed_stockNo(industry_code_list):
        print(stockNo)
        adjust_stock_data(stockNo, data_start_date, data_end_date)

    print('time_daily_update:', time.time() - time_daily_update)
