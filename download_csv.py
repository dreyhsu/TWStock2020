import datetime
from stock_db import create_price_table, insert_db
from crawler import crawler, crawler_te
import sqlite3
import pandas as pd

path = 'C:/sqlite/stock.db'


def trans_date(date_time):
    """Transform datetime object to string object"""
    return ''.join(str(date_time).split(' ')[0].split('-'))


def date_range(start_date, end_date):
    """make %Y%m%d date list"""
    start_date = datetime.date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:]))
    end_date = datetime.date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:]))
    date_list = []
    for n in range(1, (end_date - start_date).days + 1):
        date_list.append((start_date + datetime.timedelta(n)).strftime("%Y%m%d"))
    return date_list


def main():
    # create_price_table()  # 先確定price table已經存在
    # 現在起始日期20181024
    try:
        conn = sqlite3.connect(path)
        date_series = pd.read_sql(con=conn, sql="SELECT DISTINCT Date FROM price")  # price table Date 排序，取最後一個日期
        conn.commit()
    finally:
        conn.close()
    last_date = date_series['Date'].iloc[-1]
    print(f'Last day {last_date}')
    start = str(last_date)
    # start = '20181023'
    print("start day is {}".format(start))
    end = datetime.datetime.now().strftime("%Y%m%d")
    # end = '20200925'
    print("End day is {}".format(end))
    if end > start:
        date_list = date_range(start, end)
        print('We will download : from {} to {}'.format(date_list[0], date_list[-1]))
        for i in date_list:
            df = crawler(i)
            df_te = crawler_te(i)
            if df is not None:
                insert_db(df, 'price')  # 新爬到的資料insert進price table
            if df_te is not None:
                insert_db(df_te, 'te_price')  # 新爬到的上櫃資料insert進te_price table
    else:
        print("Database has already been updated.")

    # conn = sqlite3.connect(path)
    # date_series = pd.read_sql(con=conn, sql="SELECT DISTINCT Date FROM te_price")  # price table Date 排序，取最後一個日期
    # conn.commit()
    # conn.close()
    # last_date = date_series['Date'].iloc[-1]
    # print(f'Last day {last_date}')
    # start = str(last_date)
    # print("start day is {}".format(start))
    # end = datetime.datetime.now().strftime("%Y%m%d")
    # # print("End day is {}".format(end))
    # if end > start:
    #     date_list = date_range(start, end)
    #     print('We will download : from {} to {}'.format(date_list[0], date_list[-1]))
    #     for i in date_list:
    #         df = crawler_te(i)
    #         if df is not None:
    #             insert_db(df, 'te_price')
    # else:
    #     print("Database has already been updated.")

if __name__ == '__main__':
    main()
