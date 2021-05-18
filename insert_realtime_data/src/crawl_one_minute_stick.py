import json
import asyncio
from aiohttp import ClientSession, TCPConnector, ClientTimeout
import datetime
import tenacity
from sqlalchemy import create_engine
import pytz
from .config import settings
tpe = pytz.timezone('Asia/Taipei')

mysql_engine = create_engine(settings.SQLALCHEMY_WAREHOUSE_URI, pool_pre_ping=True)

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
          INSERT INTO TaiwanStockOneMinuteK (stock_id, date_time, open, close, high, low, volume)
          VALUES (%(stock_id)s, %(date_time)s, %(open)s, %(close)s, %(high)s, %(low)s, %(volume)s)
          ON DUPLICATE KEY UPDATE open = open, close = close, high = high, low = low, volume = volume;
          """

    with mysql_engine.connect() as connection:
        _ = connection.execute(sql, data)


def aggregate_data(json_dict):
    times = json_dict['data']['t']
    opens = json_dict['data']['o']
    highs = json_dict['data']['h']
    lows = json_dict['data']['l']
    closes = json_dict['data']['c']
    volumes = json_dict['data']['v']
    quote = json_dict['data']['quote']
    if times != []:
        return {
            'date_time': datetime.datetime.strftime(datetime.datetime.fromtimestamp(times[0]).astimezone(tpe), '%Y-%m-%d %H:%M:%S'),
            'stock_id': quote["0"].split(':')[1],
            'open': opens[0],
            'close': closes[0],
            'high': highs[0],
            'low': lows[0],
            'volume': volumes[0]
        }
    return None


@tenacity.retry(stop=tenacity.stop_after_attempt(10))
async def get_url(i, url, session):
    async with session.get(url) as response:
        r = await response.text()
        one_minute_data = aggregate_data(json.loads(r))
        if one_minute_data:
            config.data.append(one_minute_data)


async def run_asyncio(stocks_id):
    urls = []
    for stock_id in stocks_id:
        urls.append(f'https://ws.api.cnyes.com/ws/api/v1/charting/history?symbol=TWS:{stock_id}:STOCK&resolution=1&quote=1')

    timeout = ClientTimeout(total=5)
    client_session = ClientSession(connector=TCPConnector(verify_ssl=False), timeout=timeout)
    tasks = [asyncio.create_task(get_url(i, url, client_session)) for i, url in enumerate(urls)]
    await asyncio.gather(*tasks)


def run():
    config.data = []
    stocks_id = get_stocks_id_from_db()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_asyncio(stocks_id))
    insert_to_db(config.data)
    return len(config.data)
