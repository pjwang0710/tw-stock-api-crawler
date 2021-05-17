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

with open('proxies.txt', 'r') as f:
    proxies = f.read().split('\n')

class config:
    data = []

def get_stocks_id_from_db():
    sql = "SELECT stock_id from TaiwanStockFiveMinuteStockInfo"
    with mysql_engine.connect() as connection:
        result = connection.execute(sql).fetchall()
    return [item[0] for item in result]


def insert_to_db(data):
    sql = """
          INSERT INTO TaiwanStockFiveMinuteStock (date, stock_id, sold_out_price, sold_out_count, buy_in_price, buy_in_count, accumulate_transaction_count, time, max_price, min_price, open_price, deal_price, deal_count)
          VALUES (%(d)s, %(c)s, %(a)s, %(f)s, %(b)s, %(g)s, %(v)s, %(t)s, %(h)s, %(l)s, %(o)s, %(z)s, %(s)s)
          ON DUPLICATE KEY UPDATE sold_out_price = sold_out_price, sold_out_count = sold_out_count, buy_in_price = buy_in_price, buy_in_count = buy_in_count, accumulate_transaction_count = accumulate_transaction_count, max_price = max_price, min_price = min_price, open_price = open_price, deal_price = deal_price, deal_count = deal_count;
          """
    
    with mysql_engine.connect() as connection:
        result = connection.execute(sql, data)


@tenacity.retry(stop=tenacity.stop_after_attempt(10))
async def get_url(i, url, session):
    proxy = random.choice(proxies)
    now = time.time()
#     , proxy=proxy
    async with session.get(url) as response:
        r = await response.text()
        config.data.append(json.loads(r))
#         for d in config.data:
#             if 'd' not in d.keys():
#                 d['d'] = ''
#             if 'c' not in d.keys():
#                 d['c'] = ''
#             if 'a' not in d.keys():
#                 d['a'] = ''
#             if 'f' not in d.keys():
#                 d['f'] = ''
#             if 'b' not in d.keys():
#                 d['b'] = ''
#             if 'g' not in d.keys():
#                 d['g'] = ''
#             if 'v' not in d.keys():
#                 d['v'] = ''
#             if 't' not in d.keys():
#                 d['t'] = ''
#             if 'h' not in d.keys():
#                 d['h'] = ''
#             if 'l' not in d.keys():
#                 d['l'] = ''
#             if 'o' not in d.keys():
#                 d['o'] = ''
#             if 'z' not in d.keys():
#                 d['z'] = ''
#             if 's' not in d.keys():
#                 d['s'] = ''
#         insert_to_db(config.data)
    print(i, len(config.data))

async def run_asyncio(stocks_id):
    urls = []
    for stock_id in stocks_id:
        urls.append(f'https://ws.api.cnyes.com/ws/api/v1/charting/history?symbol=TWS:{stock_id}:STOCK&resolution=1')

    timeout = ClientTimeout(total=5)
    client_session = ClientSession(connector=TCPConnector(verify_ssl=False), timeout=timeout)
    tasks = [asyncio.create_task(get_url(i, url, client_session)) for i, url in enumerate(urls)]
    await asyncio.gather(*tasks)


def run():
    stocks_id = get_stocks_id_from_db()
    loop = asyncio.get_event_loop()
    now = time.time()
    loop.run_until_complete(run_asyncio(stocks_id))
    return len(config.data)

now = time.time()
run()
print(time.time() - now)
