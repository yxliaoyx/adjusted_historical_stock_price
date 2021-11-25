import os
import pandas as pd


def get_listed_stockNo(industry_code_list):
    stockNo_set = set()
    for industry_code in industry_code_list:
        try:
            csv_file = os.path.join('data', 'listed_company', f'{industry_code}.csv')
            df = pd.read_csv(csv_file, dtype=str, encoding='cp950')
            stockNo_set.update(df.iloc[:, 2])
        except Exception as e:
            print(e)
            continue

    return stockNo_set


def get_listing_date():
    listing_date_dict = dict()
    for industry_code in range(35):
        try:
            csv_file = os.path.join('data', 'listed_company', f'{industry_code}.csv')
            df = pd.read_csv(csv_file, dtype=str, encoding='cp950')
            listing_date_dict.update(dict(df[['有價證券代號', '公開發行/上市(櫃)/發行日']].values))
        except Exception as e:
            print(e)
            continue

    return listing_date_dict


def get_stock_data(stockNo, start_date, end_date):
    data = pd.DataFrame()

    for pd_period in pd.period_range(start=start_date, end=end_date, freq='M'):
        stockDate = f'{pd_period.year}{pd_period.month:02}01'
        try:
            csv_file = os.path.join('data', 'stock_csv', stockNo, f'{stockNo}_{stockDate}.csv')
            dtype_dict = {'成交股數': str, '成交金額': str, '開盤價': str, '最高價': str,
                          '最低價': str, '收盤價': str, '漲跌價差': str, '成交筆數': str}
            df = pd.read_csv(csv_file, dtype=dtype_dict, engine='python', encoding='cp950', skiprows=1, skipfooter=4)
        except Exception as e:
            print(e)
            continue

        df.drop(df.index[df['日期'] == '說明:'], inplace=True)

        df['日期'] = df.apply(lambda x: x['日期'].replace(x['日期'][0:3], str(int(x['日期'][0:3]) + 1911)), axis=1)
        df['日期'] = pd.to_datetime(df['日期'])

        df['成交股數'] = df['成交股數'].str.replace(',', '').astype(float)
        df['成交金額'] = df['成交金額'].str.replace(',', '').astype(float)
        df['開盤價'] = df['開盤價'].str.replace('--', '0')
        df['開盤價'] = df['開盤價'].str.replace(',', '').astype(float)
        df['最高價'] = df['最高價'].str.replace('--', '0')
        df['最高價'] = df['最高價'].str.replace(',', '').astype(float)
        df['最低價'] = df['最低價'].str.replace('--', '0')
        df['最低價'] = df['最低價'].str.replace(',', '').astype(float)
        df['收盤價'] = df['收盤價'].str.replace('--', '0')
        df['收盤價'] = df['收盤價'].str.replace(',', '').astype(float)
        df['漲跌價差'] = df['漲跌價差'].str.replace('X', '').astype(float)
        df['成交筆數'] = df['成交筆數'].str.replace(',', '').astype(float)

        data = data.append(df, ignore_index=True)

    return data[(data['日期'] >= start_date) & (data['日期'] <= end_date)]


def get_XRXD_data():
    def replace_year(x):
        x = x['資料日期'].replace(x['資料日期'][:-7], str(int(x['資料日期'][:-7]) + 1911))
        return x

    csv_file = os.path.join('data', 'XRXD_csv', 'XRXD.csv')
    dtype_dict = {'股票代號': str, '除權息前收盤價': str, '減除股利參考價': str}

    df = pd.read_csv(csv_file, dtype=dtype_dict, engine='python', encoding='cp950', skiprows=1, skipfooter=13)

    df['資料日期'] = df.apply(replace_year, axis=1)
    df['資料日期'] = df.apply(lambda x: x['資料日期'].replace('年', '').replace('月', '').replace('日', ''), axis=1)
    df['資料日期'] = pd.to_datetime(df['資料日期'])

    df['除權息前收盤價'] = df['除權息前收盤價'].str.replace(',', '').astype(float)
    df['減除股利參考價'] = df['減除股利參考價'].str.replace(',', '').astype(float)

    return df


def get_reduction_data():
    def replace_year(x):
        x = x['恢復買賣日期'].replace(x['恢復買賣日期'][0:3], str(int(x['恢復買賣日期'][0:3]) + 1911))
        return x

    csv_file = os.path.join('data', 'reduction_csv', 'reduction.csv')
    dtype_dict = {'股票代號': str, '停止買賣前收盤價格': str, '恢復買賣參考價': str}

    df = pd.read_csv(csv_file, dtype=dtype_dict, engine='python', encoding='cp950', skiprows=1, skipfooter=12)

    df['恢復買賣日期'] = df.apply(replace_year, axis=1)
    df['恢復買賣日期'] = pd.to_datetime(df['恢復買賣日期'])

    df['停止買賣前收盤價格'] = df['停止買賣前收盤價格'].str.replace(',', '').astype(float)
    df['恢復買賣參考價'] = df['恢復買賣參考價'].str.replace(',', '').astype(float)

    return df


def get_adjusted_stock_data(stockNo, start_date, end_date):
    csv_file = os.path.join('data', 'adjusted_stock_data', f'{stockNo}.csv')
    df = pd.read_csv(csv_file, encoding='cp950')
    df['日期'] = pd.to_datetime(df['日期'])

    return df[(df['日期'] >= start_date) & (df['日期'] <= end_date)]
