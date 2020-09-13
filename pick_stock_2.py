import pandas as pd
import pickle
import numpy as np
import shutil
import os
import mpl_finance as mpf
import talib
import sqlite3
import time
import matplotlib.pyplot as plt
from stock_db import id_list, fetch_db
import pandas as pd


path = 'C:/sqlite/stock.db'

def fig_plot(df, stock_id, pic_folder):
    n = -200
    closes = np.array(df['Close'].iloc[n:].tolist())
    highs = np.array(df['High'].iloc[n:].tolist())
    lows = np.array(df['Low'].iloc[n:].tolist())
    opens = np.array(df['Open'].iloc[n:].tolist())
    Time = np.array(df['Date'].map(lambda x: str(x)).iloc[n:].tolist())
    Vol = np.array(df['Volume'].iloc[n:].tolist())
    #use talib pakage to calculate SMA series
    avg60 = talib.SMA(closes, timeperiod=60)
    avg120 = talib.SMA(closes, timeperiod=120)
    avg150 = talib.SMA(closes, timeperiod=150)
    #create plot figure
    fig = plt.figure()
    ax1 = plt.subplot(2,1,1)
    ax2 = plt.subplot(2,1,2)
    #x label range
    ax1.set_xticks(range(0, len(Time), 15))
    ax1.set_xticklabels(Time[::10], rotation=40)
    mpf.candlestick2_ochl(ax1, opens, closes, highs, lows, width=0.6, colorup='r', colordown='g')
    #SMA line
    ax1.plot(avg60, 'g', label='MA60')
    ax1.plot(avg120, 'k--', label='MA120')
    ax1.plot(avg150, 'k.', label='MA150')
    #價量結構
    for n in [10, 50]:
        MaxVolClose = df['Close'].iloc[df['Volume'].iloc[-n:].idxmax()]
        MaxVolOpen = df['Open'].iloc[df['Volume'].iloc[-n:].idxmax()]
        if MaxVolClose > MaxVolOpen:
            ax1.axhline(y=MaxVolOpen, c='r', ls='--', lw=2, label=f'support at {MaxVolOpen}')
        else:
            ax1.axhline(y=MaxVolClose, c='b', ls='--', lw=2, label=f'pressure at {MaxVolClose}')
    #成交量
    ax2.bar(Time, Vol ,color='b')
    ax2.set_xticks(range(0, len(Time), 15))
    ax2.set_xticklabels(Time[::10], rotation=40)
    #圖片網紋
    ax1.grid(True, ls=':', color='k', alpha=0.5)
    ax2.grid(True, ls=':', color='k', alpha=0.5)
    #fig setting
    plt.rc('figure', figsize=(10, 10))
    plt.title(stock_id)
    plt.subplots_adjust(wspace=0, hspace=0)
    ax1.legend(loc = 'best')
    fig_path = 'C:/Users/user/Pictures/'+pic_folder+'/' + stock_id.split('.')[0] + '.png'
    plt.savefig(fig_path)
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
    def above_ma5_closeunder20(df, TargetPrice):
        try:
            close = df['Close']
            avg60 = talib.SMA(close, timeperiod=60)
            avg120 = talib.SMA(close, timeperiod=120)
            avg150 = talib.SMA(close, timeperiod=150)
            df['MA60'] = avg60
            df['MA120'] = avg120
            df['MA150'] = avg150
            if df['Volume'].iloc[-1] > 200000 and TargetPrice >= df['Close'].iloc[-1] \
                    and df['MA60'].iloc[-1] > df['MA60'].iloc[-2] and df['MA120'].iloc[-1] > df['MA120'].iloc[-2]\
                    and df['MA150'].iloc[-1] > df['MA150'].iloc[-2] and df['MA60'].iloc[-1] > df['MA120'].iloc[-1] \
                    > df['MA150'].iloc[-1]:
                return True
        except (IndexError, np.linalg.linalg.LinAlgError):
            return False
        except:
            return False

    #deal with folders
    for Folders in ['for_drey', 'for_mandy']:
        folder = 'C:/Users/user/Pictures/'+Folders
        if not os.path.exists(folder):
            os.makedirs(folder)
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))

    #Make stock ID list
    table_list = ['price', 'te_price']
    for table in table_list:
        lstFiles = id_list(table)
        for stock_id in lstFiles:
            print(stock_id)
            try:
                df = fetch_db(stock_id, table)
                # if above_ma5_closeunder20(df, TargetPrice=15):
                #     fig_plot(df, stock_id, pic_folder='for_drey')
                if above_ma5_closeunder20(df, TargetPrice=70):
                    fig_plot(df, stock_id, pic_folder='for_mandy')

            except FileNotFoundError:
                print("{} is not found.".format(str(stock_id)))


if __name__ == '__main__':
    pick_day_stock()