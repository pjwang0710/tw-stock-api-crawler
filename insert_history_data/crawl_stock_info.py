import requests
import json
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv('.env')

SQLALCHEMY_DATALAKE_URI = os.getenv('SQLALCHEMY_DATALAKE_URI')
mysql_engine = create_engine(SQLALCHEMY_DATALAKE_URI, pool_pre_ping=True)

def insert_to_db(data):
    sql = """
          INSERT INTO TaiwanStockPrice ()
          VALUES ()
          ON DUPLICATE KEY UPDATE;
          """

    with mysql_engine.connect() as connection:
        result = connection.execute(sql, data)


def run():
    url = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
    r = requests.get(url)
    df_list = pd.read_html(r.text, header=None) 

    rename_pair = {
        0: 'raw_stock_id',
        1: 'isin_code',
        2: 'start_date',
        3: 'market_type',
        4: 'industry_type',
        5: 'cfi_code',
        6: 'note'
    }
    df = df_list[0]
    df = df[df[3] == '上市']
    df = df.fillna('')
    df = df.rename(columns=rename_pair)
    df['raw_stock_id'] = df['raw_stock_id'].apply(lambda x: x.replace(' ', '　'))
    df['start_date'] = df['start_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y/%m/%d'))
    df['stock_id'] = df['raw_stock_id'].apply(lambda x: x.split('　')[0])
    df['stock_name'] = df['raw_stock_id'].apply(lambda x: x.split('　')[1])
    return df.T.dict()

