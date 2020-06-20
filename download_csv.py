import requests
import pandas as pd
import io
import time
import datetime
import random
import sys
from stock_db import create_db, insert_price
import sqlite3
from fake_useragent import UserAgent


def crawler(date_time):
    """Crawl daily stock data from TWSE"""
    def _str_to_float(x):
        """The raw data of price is a string object, we need to trans it to float."""
        try:
            x = pd.to_numeric(x)
            return x
        except ValueError:
            return -1

    page_url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + date_time +'&type=ALLBUT0999'
    ua = UserAgent()
    try:
        headers = {"User-Agent" : ua.random}
        page = requests.get(page_url, headers = headers)
        use_text = page.text.splitlines()
        for i,text in enumerate(use_text):
            if text == '"證券代號","證券名稱","成交股數","成交筆數","成交金額","開盤價","最高價","最低價","收盤價","漲跌(+/-)","漲跌價差","最後揭示買價","最後揭示買量","最後揭示賣價","最後揭示賣量","本益比",':
                initial_point = i
                break
        df = pd.read_csv(io.StringIO(''.join([text[:-1] + '\n' for text in use_text[initial_point:]])))
        """Manipulate raw data"""
        df['Date'] = date_time
        df['證券代號'] = df['證券代號'].apply(lambda x:x.replace('"',''))
        df['證券代號'] = df['證券代號'].apply(lambda x: x.replace('=',''))
        df['成交股數'] = pd.to_numeric(df['成交股數'].apply(lambda x: x.replace(',', ''))).astype(float)
        OHLC = ["開盤價","最高價","最低價","收盤價"]
        for i in OHLC:
            df[i] = df[i].apply(_str_to_float)
        df = df[["Date","證券代號","成交股數","開盤價","最高價","最低價","收盤價"]]
        df.columns = ["Date","ID","Volume","Open","High","Low","Close"]
        print("Scessfully download " + (date_time))
    except UnboundLocalError:
        """If date is an off-work day."""
        print(str(date_time) + ' is offwork day!')
        return None
    except ConnectionError:
        """If blocked by TWSE, we keep download csv after 10 secs break."""
        print(str(date_time) + ' Connect error, will try again in 10 secs')
        time.sleep(10)
        crawler(date_time)
    except Exception as e:
        print(e)
        e_type, e_value, e_tb = sys.exc_info()
        print("type:{}\nmessage:{}\ninfo:{}\n".format(e_type, e_value, e_tb))

    create_db(df)
    insert_price(df)

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
    conn = sqlite3.connect('C:/Users/user/Documents/stocks/0050.db')
    c = conn.cursor()
    c.execute("SELECT 'Date' FROM price ORDER BY 'Date' DESC")
    conn.commit()
    df = pd.read_sql(con=conn, sql="SELECT * FROM price")
    conn.close()
    last_date = df['Date'].iloc[-1]
    print("Last day {}".format(last_date))
    start = last_date
    # print("start day is {}".format(start))
    end = datetime.datetime.now().strftime("%Y%m%d")
    # print("End day is {}".format(end))
    if end > start:
        date_list = date_range(start, end)
        print('We will download : from {} to {}'.format(date_list[0], date_list[-1]))
        user_confirm = input("update database? y/n  ")
        if user_confirm == 'y':
            for i in date_list:
                crawler(i)
        else:
            print("Download stop")
    else:
        print("Database has already been updated.")

if __name__ == '__main__':
    main()
    