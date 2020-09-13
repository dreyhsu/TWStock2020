import requests
import pandas as pd
import io
import time
import random
import sys
from fake_useragent import UserAgent


def crawler(date_time):
    """Crawl daily stock data from TWSE"""
    def _str_to_float(x):
        """The raw data of price is a string object, we need to trans it to float."""
        try:
            if ',' in x:
                x = x.replace(',', '')
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
            if text == '"證券代號","證券名稱","成交股數","成交筆數","成交金額","開盤價","最高價","最低價",' \
                       '"收盤價","漲跌(+/-)","漲跌價差","最後揭示買價","最後揭示買量","最後揭示賣價","最後揭示賣量","本益比",':
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
        print("Successfully download TWSE " + date_time)
        time.sleep(random.randint(7, 14))
        # 回傳下載明細
        return df
    except UnboundLocalError:
        """If date is an off-work day."""
        print(str(date_time) + ' is offwork day!')
        time.sleep(random.randint(7, 14))
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
        crawler(date_time)


def crawler_te(date_time):
    def _str_to_float(x):
        """The raw data of price is a string object, we need to trans it to float."""
        try:
            if ',' in x:
                x = x.replace(',', '')
            x = pd.to_numeric(x)
            return x
        except ValueError:
            return -1

    date_split = [str(int(date_time[0:4]) - 1911), (date_time[4:6]), (date_time[6:8])]
    page_url = "http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/" \
               "stk_quote_result.php?l=zh-tw&o=csv&d={}/{}/{}&s=0,asc,0".format(date_split[0], date_split[1],
                                                                                date_split[2])
    ua = UserAgent()
    try:
        headers = {"User-Agent": ua.random}
        page = requests.get(page_url, headers=headers)
        use_text = page.text.splitlines()
        df = pd.read_csv(io.StringIO(''.join([text + '\n' for text in use_text[2:-8]])))
        df = df[df['代號'].map(len) == 4]   # 目前只取代號長度為4的股票
        df['Date'] = date_time
        df['成交股數  '] = pd.to_numeric(df['成交股數  '].map((lambda x: x.replace(',', '')))).astype(float)
        OHLC = ["開盤 ", "最高 ", "最低", "收盤 "]
        for i in OHLC:
            df[i] = df[i].apply(_str_to_float)
        df = df[["Date", "代號", "成交股數  ", "開盤 ", "最高 ", "最低", "收盤 "]]
        df.columns = ["Date", "ID", "Volume", "Open", "High", "Low", "Close"]
        print("Successfully download TE " + date_time)
        time.sleep(random.randint(7, 14))
        return df
    except pd.errors.EmptyDataError:
        """If date is an off-work day."""
        print(str(date_time) + ' is offwork day!')
        time.sleep(random.randint(7, 14))
        return None
    except ConnectionError:
        """If blocked by TWSE, we keep download csv after 10 secs break."""
        print(str(date_time) + ' Connect error, will try again in 10 secs')
        time.sleep(10)
        crawler_te(date_time)
    except Exception as e:
        print(e)
        e_type, e_value, e_tb = sys.exc_info()
        print("type:{}\nmessage:{}\ninfo:{}\n".format(e_type, e_value, e_tb))
        crawler_te(date_time)