import sqlite3
import os
import pandas as pd

path = 'db/stock.db'

def create_price_table():
    # need to change folder
    if os.path.exists(path):
        pass
    else:
        print(f'createDB table price')
        conn = sqlite3.connect(path)
        c = conn.cursor()
        # 新增 ID 欄位
        c.execute("""CREATE TABLE price (
                    Date DATE,
                    ID text,
                    Volume real,
                    Open real,
                    High real,
                    Low real,
                    Close real
                    )""")
        conn.commit()
        conn.close()


def create_te_price_table():
    path = 'db/stock.db'
    try:
        print(f'createDB table price')
        conn = sqlite3.connect(path)
        c = conn.cursor()
        # 新增 ID 欄位
        c.execute("""CREATE TABLE te_price (
                    Date DATE,
                    ID text,
                    Volume real,
                    Open real,
                    High real,
                    Low real,
                    Close real
                    )""")
        conn.commit()
    finally:
        conn.close()


def create_pick_table(select_type):
    # need to change folder
    conn = sqlite3.connect(path)
    c = conn.cursor()
    # 新增 ID 欄位
    c.execute(f'CREATE TABLE {select_type} ('
                """ 
                Date DATE,
                ID text
                )""")
    conn.commit()
    conn.close()


def insert_select_db(df, stock_id):
    conn = sqlite3.connect(path)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO price VALUES (:Date,:ID)",
                  {'Date': df['Date'].iloc[-1],
                   'ID': stock_id})
    finally:
        conn.commit()
        conn.close()


def insert_db(df, table_name):
    conn = sqlite3.connect(path)
    c = conn.cursor()
    try:
        for i in range(len(df['ID'])):
            if df['Open'].iloc[i] == -1:
                continue
            else:
                c.execute(f'INSERT INTO {table_name} VALUES (:Date,:ID , :Volume, :Open, :High, :Low, :Close)',
                          {'Date': df['Date'].iloc[i],
                           'ID': df['ID'].iloc[i],
                           'Volume': df['Volume'].iloc[i],
                           'Open': df['Open'].iloc[i],
                           'High': df['High'].iloc[i],
                           'Low': df['Low'].iloc[i],
                           'Close': df['Close'].iloc[i]})
    finally:
        conn.commit()
        conn.close()


def fetch_db(stock_id, table):
    conn = sqlite3.connect(path)
    try:
        #     dOut = pd.read_sql(con=conn, sql="SELECT * FROM price")
        df = pd.read_sql(con=conn, sql=(f'SELECT Date,Close,Open,High,Low,Volume FROM {table} '
                                        f'WHERE ID = "{stock_id}"'))
        df = df.sort_values(by='Date')
        return df
    finally:
        conn.close()


def fetch_date():
    conn = sqlite3.connect(path)
    try:
        #     dOut = pd.read_sql(con=conn, sql="SELECT * FROM price")
        df = pd.read_sql(con=conn, sql=(f'SELECT Date FROM price '
                                        f'WHERE ID = "0050"'))
        df = df.sort_values(by='Date')
        return df
    finally:
        conn.close()


def id_list(table):
    conn = sqlite3.connect(path)
    try:
        df = pd.read_sql(con=conn, sql=(f'SELECT DISTINCT ID FROM {table} '))
        return df['ID']
    finally:
        conn.close()