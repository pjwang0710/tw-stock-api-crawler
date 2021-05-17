import json
import time
import asyncio
from aiohttp import ClientSession, TCPConnector, ClientTimeout
import random
import time
import tenacity
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv('.env')

SQLALCHEMY_DATALAKE_URI = os.getenv('SQLALCHEMY_DATALAKE_URI')
mysql_engine = create_engine(SQLALCHEMY_DATALAKE_URI, pool_pre_ping=True)

with open('stocks_id.json', 'r') as f:
    stocks_id = json.loads(f.read())

with open('proxies.txt', 'r') as f:
    proxies = f.read().split('\n')

def insert_to_db(data):
    sql = """
          INSERT INTO TaiwanStockPrice (date, stock_id, Trading_Volume, Trading_money, open, max, min, close, spread, Trading_turnover)
          VALUES (%(date)s, %(stock_id)s, %(Trading_Volume)s, %(Trading_money)s, %(open)s, %(max)s, %(min)s, %(close)s, %(spread)s, %(Trading_turnover)s)
          ON DUPLICATE KEY UPDATE Trading_Volume = Trading_Volume, Trading_money = Trading_money, open = open, max = max, min = min, close = close, spread = spread, Trading_turnover = Trading_turnover;
          """
    
    with mysql_engine.connect() as connection:
        result = connection.execute(sql, data)


@tenacity.retry(stop=tenacity.stop_after_attempt(10))
async def get_url(i, url, session, params):
    proxy = random.choice(proxies)
    now = time.time()
    async with session.get(url, params=params) as response:
        r = await response.text()
        data = json.loads(r)['data']
        if len(data) != 0:
            insert_to_db(data)
    print(i, time.time() - now)


async def run_asyncio(stocks_id):
    urls = []
    parameters = []
    for i, stock_id in enumerate(stocks_id):
        url = "https://api.finmindtrade.com/api/v4/data"
        parameters.append({
            "dataset": "TaiwanStockPrice",
            "data_id": stock_id,
            "start_date": "2001-01-01",
            "end_date": "2021-05-14",
            "token": ""
        })
        urls.append(url)
    timeout = ClientTimeout(total=1000)
    client_session = ClientSession(connector=TCPConnector(verify_ssl=False), timeout=timeout)
    tasks = [asyncio.create_task(get_url(i, url, client_session, parameters[i])) for i, url in enumerate(urls)]
    await asyncio.gather(*tasks)


loop = asyncio.get_event_loop()
now = time.time()
loop.run_until_complete(run_asyncio(stocks_id))
print(time.time() - now)
