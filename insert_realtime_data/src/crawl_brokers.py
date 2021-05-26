import asyncio
from aiohttp import ClientSession, TCPConnector, ClientTimeout
import random
import tenacity
from sqlalchemy import create_engine
# from .config import settings
from bs4 import BeautifulSoup
import re
import datetime

SQLALCHEMY_WAREHOUSE_URI = 'mysql+pymysql://pj:#%Rfb_7)Y<6k3-TP"TY?e6Dv:J6K[;,X@18.181.48.71:3306/stock_api_warehouse'
mysql_engine = create_engine(SQLALCHEMY_WAREHOUSE_URI, pool_pre_ping=True)

with open('proxies.txt', 'r') as f:
    proxies = f.read().split('\n')


class config:
    data = 0


def get_brokers():
    with open('brokers.txt', 'r') as f:
        brokers = f.read().split('\n')
        brokers = [item.split(',') for item in brokers]
    return brokers


def insert_to_db(data):
    sql = """
          INSERT INTO TaiwanBrokerExchange (securities_id, branch_id, broker_id, broker_name, buy_in, sold_out, delta, year, month, day)
          VALUES (%(securities_id)s, %(branch_id)s, %(broker_id)s, %(broker_name)s, %(buy_in)s, %(sold_out)s, %(delta)s, %(year)s, %(month)s, %(day)s)
          ON DUPLICATE KEY UPDATE broker_name = broker_name, buy_in = buy_in, sold_out = sold_out, delta = delta;
          """

    with mysql_engine.connect() as connection:
        _ = connection.execute(sql, data)


def parse_html(raw_html, broker_id, branch_id, year, month, day):
    soup = BeautifulSoup(raw_html, 'html.parser')
    try:
        mainTable = soup.findAll('table', {'id': 'oMainTable'})[0]
    except Exception:
        return None
    assert len(mainTable.findAll('table')) == 2
    company_name_re1 = re.compile('GenLink2stk(\(.*?\))')
    company_name_re2 = re.compile("'(.*)?'")
    broker_dicts = []
    for table in mainTable.findAll('table'):
        if '無此券商分點交易資料' in table.text:
            break
        for tr in table.findAll('tr')[2:]:
            company_name_script = tr.find('script')
            if company_name_script is not None:
                company_name = company_name_re1.findall(str(tr.find('script')))[0]
                company_name = eval(company_name)
            else:
                merged_name = tr.find('a').text
                company_id = company_name_re2.findall(tr.find('a')['href'])[0]
                company_name = (company_id, merged_name.replace(company_id, ''))
            broker_dicts.append({
                'securities_id': broker_id,
                'branch_id': branch_id,
                'broker_id': company_name[0],
                'broker_name': company_name[1],
                'buy_in': int(tr.findAll('td')[-3].text.replace(',', '')),
                'sold_out': int(tr.findAll('td')[-2].text.replace(',', '')),
                'delta': int(tr.findAll('td')[-1].text.replace(',', '')),
                'year': year,
                'month': month,
                'day': day
            })
    return broker_dicts


<<<<<<< HEAD
#@tenacity.retry(stop=tenacity.stop_after_attempt(10))
=======
@tenacity.retry(stop=tenacity.stop_after_attempt(10))
>>>>>>> 1a6e9e23d97e7e246155b461afe7ae2fad9f5636
async def get_url(i, url, session):
    proxy = random.choice(proxies)
    async with session.get(url[0], proxy=proxy) as response:
        r = await response.text(encoding='big5-hkscs')
        data = parse_html(r, url[1], url[2], url[3], url[4], url[5])
        if data != []:
            insert_to_db(data)
            config.data += len(data)


async def run_asyncio(brokers, date_ptr):
    year = date_ptr.year
    month = date_ptr.month
    day = date_ptr.day
    urls = []
    for broker_branch_id in brokers:
        urls.append(['https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm?a=%s&b=%s&c=B&e=%s-%s-%s&f=%s-%s-%s' % (broker_branch_id[0], broker_branch_id[1], year, month, day, year, month, day), broker_branch_id[0], broker_branch_id[1], year, month, day])
    timeout = ClientTimeout(total=60)
    client_session = ClientSession(connector=TCPConnector(verify_ssl=False), timeout=timeout)
    tasks = [asyncio.create_task(get_url(i, url, client_session)) for i, url in enumerate(urls)]
    await asyncio.gather(*tasks)


def run(date_ptr):
    brokers = get_brokers()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_asyncio(brokers, date_ptr))
    return config.data

run(datetime.datetime.now() - datetime.timedelta(days=1))
