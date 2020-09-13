import pandas as pd
import pickle
import numpy as np
import shutil
from os import listdir
import mplfinance as mpf
import talib
import sqlite3
import time
import matplotlib.pyplot as plt

def fig_plot(df, stock_id):
    df['Date'] = pd.to_datetime(df['Date'])
    df.index = df['Date']
    print('Ploting ', stock_id.split('.')[0])
    fig_path = 'C:/Users/user/Pictures/stock_pic/'+ stock_id.split('.')[0] +'.png'
    mpf.plot(df[-50:], type='candle', mav=(5,20), volume = True, title = stock_id, savefig = fig_path)
    plt.close()


# def calculate_slope(ys):
#     xs = list(range(len(ys)))
#     xs = np.array(xs)
#     ys = np.array(ys)
#     A = np.vstack([xs, np.ones(len(xs))]).T
#     m, c = np.linalg.lstsq(A, ys, rcond=None)[0]
#     return m
#
#
# def roughness(ys):
#     avg = sum(ys)/len(ys)
#     peak = max(ys)
#     if (peak - avg)/avg < 0.1:
#         return True
#     else:
#         return False


# class Stock:
#     def __init__(self, stock_id):
#         conn = sqlite3.connect("C:/Users/user/Documents/stocks/{}.db".format(stock_id))
#         c = conn.cursor()
#         c.execute("SELECT 'Date' FROM price ORDER BY 'Date' DESC")
#         conn.commit()
#         self.stock_id = stock_id
#         self.df = pd.read_sql(con=conn, sql="SELECT * FROM price")
#         conn.close()
#
#     def ma20_bulltrend(self):
#         try:
#             n = 20
#             ma20 = self.df['MA20'].iloc[-n:].tolist()
#             slope_20 = calculate_slope(ma20)
#             if self.df['Volume'].iloc[-1] > 100000 and abs((slope_10 - slope_20) / slope_20) < dif and slope_10 > 0 \
#                     and slope_20 > 0 and _if_all_above():
#                 return True
#         except (IndexError, np.linalg.linalg.LinAlgError):
#             return False


def pick_day_stock():
    def above_ma5_closeunder20(df):
        try:
            close = df['Close']
            avg5 = talib.SMA(close, timeperiod=5)
            #avg20 = talib.SMA(close, timeperiod=20)
            df['MA5'] = avg5
            #df['MA20'] = avg20
            if df['Volume'].iloc[-1] > 200000 and 15 > df['Close'].iloc[-1] > df['MA5'].iloc[-1] \
                    and df['MA5'].iloc[-1] > df['MA5'].iloc[-2]:
                return True
        except (IndexError, np.linalg.linalg.LinAlgError):
            return False
        except:
            return False

    """Make stock ID list"""
    dbpath = "C:/Users/user/Documents/stocks/"
    lstFiles = listdir(dbpath)
    for stock_id in lstFiles:
        try:
            """Connect to database"""
            conn = sqlite3.connect(dbpath + stock_id)
            c = conn.cursor()
            c.execute("SELECT 'Date' FROM price ORDER BY 'Date' DESC")
            conn.commit()
            df = pd.read_sql(con=conn, sql="SELECT * FROM price")
            conn.close()
            if above_ma5_closeunder20(df):
                fig_plot(df, stock_id)

        except FileNotFoundError:
            print("{} is not found.".format(str(stock_id)))


if __name__ == '__main__':
    pick_day_stock()