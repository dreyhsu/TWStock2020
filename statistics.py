import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import talib
from stock_db import id_list, fetch_db, fetch_date


def stock_statistics():
    def diff_greater_than_zero(df, int_date):
        try:
            df['MA200'] = talib.SMA(df['Close'], timeperiod=200)
            df['diff'] = df['Close'] - df['MA200']
            date_match_list = df['Date'].loc[df['diff']>0]
            return date_match_list
        except:
            return False

    #Make stock ID list
    diff_list = []
    date_list = fetch_date()
    count_list = [0] * date_list.size
    table_list = ['price', 'te_price']
    for table in table_list:
        diff_counter = 0
        for int_date in date_list:
            lstFiles = id_list(table)
            for stock_id in lstFiles:
                try:
                    df = fetch_db(stock_id, table)
                    date_match_list = diff_greater_than_zero(df, int_date)
                    for match_date in date_match_list:
                        match_index = np.where(date_list['Date'] == match_date)[0][0]
                        count_list[match_index] += 1

                except:
                    print(f"{int_date} {stock_id} is not found.")


    Time = np.array(date_list['Date'].map(lambda x: str(x)).tolist())
    plt.plot(Time, count_list)
    plt.show()


if __name__ == '__main__':
    stock_statistics()