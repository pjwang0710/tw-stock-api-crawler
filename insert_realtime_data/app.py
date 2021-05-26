from fastapi import FastAPI
import uvicorn
from starlette_exporter import PrometheusMiddleware, handle_metrics
from prometheus_client import Counter
import datetime
import pytz
from prometheus_client.core import REGISTRY
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from insert_realtime_data.src.config import settings
from insert_realtime_data.src.crawl_one_minute_stick import run as stick_run
from insert_realtime_data.src.crawl_five_minutes_exchange import run as exchange_run
from insert_realtime_data.src.crawl_brokers import run as broker_exchange_run
from insert_realtime_data.src.crawl_cmoney import run as cmoney_run


tpe = pytz.timezone('Asia/Taipei')


scheduler = AsyncIOScheduler()

collectors = list(REGISTRY._collector_to_names.keys())
for collector in collectors:
    REGISTRY.unregister(collector)

total_one_minute_stick = Counter('crawl_one_minute_k_stick', 'Total one minute k stick count')
total_five_seconds_exchange = Counter('crawl_five_seconds_exchange', 'Total five seconds exchange')
total_brokers_exchange = Counter('crawl_brokers', 'Total brokers')
total_tradersum = Counter('total_tradersum', 'crawl total_tradersum')
total_stock_info = Counter('total_stock_info', 'crawl total_stock_info')
total_stock_revenue = Counter('total_stock_revenue', 'crawl total_stock_revenue')
total_reinvestment = Counter('total_reinvestment', 'crawl total_reinvestment')
total_per_pbr = Counter('total_per_pbr', 'crawl total_per_pbr')
total_balance_sheet = Counter('total_balance_sheet', 'crawl total_balance_sheet')
total_income_statement = Counter('total_income_statement', 'crawl total_income_statement')
total_cash_flow = Counter('total_cash_flow', 'crawl total_cash_flow')
total_financial_ratios = Counter('total_financial_ratios', 'crawl total_financial_ratios')


def get_application():
    _app = FastAPI(title=settings.PROJECT_NAME, openapi_url=f'{settings.API_V1_STR}/openapi.json')

    return _app


app = get_application()
app.add_middleware(PrometheusMiddleware)
app.add_route('/metrics', handle_metrics)


def run_crawl_one_minute_stick():
    print('start crawling one minute stick...')
    crawl_data_length = stick_run()
    total_one_minute_stick.inc(crawl_data_length)


def run_crawl_five_seconds_exchange():
    print('start crawling five seconds exchange...')
    crawl_data_length = exchange_run()
    total_five_seconds_exchange.inc(crawl_data_length)


def run_broker_exchange():
    date_ptr = datetime.datetime.now(tpe) - datetime.timedelta(days=1)
    print(f'start crawling {str(date_ptr)} bokers exchange...')
    crawl_data_length = broker_exchange_run(date_ptr)
    total_brokers_exchange.inc(crawl_data_length)


def run_crawl_tradersum():
    print('start crawling tradersum ...')
    crawl_data_length = cmoney_run('籌碼K線')
    total_tradersum.inc(crawl_data_length)


def run_crawl_stock_info():
    print('start crawling stock info...')
    crawl_data_length = cmoney_run('基本資料')
    total_stock_info.inc(crawl_data_length)


def run_crawl_stock_revenue():
    print('start crawling stock revenue...')
    crawl_data_length = cmoney_run('營收盈餘')
    total_stock_revenue.inc(crawl_data_length)


def run_crawl_reinvestment():
    print('start crawling re investment...')
    crawl_data_length = cmoney_run('轉投資')
    total_reinvestment.inc(crawl_data_length)


def run_crawl_per_pbr():
    print('start crawling PER and PBR...')
    crawl_data_length = cmoney_run('本益比')
    total_per_pbr.inc(crawl_data_length)


def run_crawl_balance_sheet():
    print('start crawling balance sheet...')
    crawl_data_length = cmoney_run('資產負債表')
    total_balance_sheet.inc(crawl_data_length)


def run_crawl_income_statement():
    print('start crawling income statement ...')
    crawl_data_length = cmoney_run('損益表')
    total_income_statement.inc(crawl_data_length)


def run_crawl_cash_flow():
    print('start crawling cash flow...')
    crawl_data_length = cmoney_run('現金流量表')
    total_cash_flow.inc(crawl_data_length)


def run_crawl_financial_ratios():
    print('start crawling financial ratios...')
    crawl_data_length = cmoney_run('財務比率')
    total_financial_ratios.inc(crawl_data_length)


@app.get('/')
async def home():
    return {'msg': 'Hello World'}


@app.on_event('startup')
async def startup():
    scheduler.start()
    scheduler.add_job(run_crawl_one_minute_stick, 'cron', hour='1-5', minute='*', second=5, id='run_crawl_one_minute_stick')
    scheduler.add_job(run_crawl_five_seconds_exchange, 'cron', hour='1-5', minute='*', second='*/15', id='run_crawl_five_seconds_exchange')
    scheduler.add_job(run_broker_exchange, 'cron', hour='17', minute='0', second='0', id='run_crawl_brokers_exchange')
    scheduler.add_job(run_crawl_tradersum, 'cron', hour='17', minute='0', second='0', id='run_crawl_tradersum')
    scheduler.add_job(run_crawl_stock_info, 'cron', day_of_week='mon', hour='0', minute='0', second='0', id='run_crawl_stock_info')
    scheduler.add_job(run_crawl_stock_revenue, 'cron', day_of_week='mon', hour='0', minute='0', second='0', id='run_crawl_stock_revenue')
    scheduler.add_job(run_crawl_reinvestment, 'cron', day_of_week='mon', hour='0', minute='0', second='0', id='run_crawl_reinvestment')
    scheduler.add_job(run_crawl_per_pbr, 'cron', day_of_week='mon', hour='0', minute='0', second='0', id='run_crawl_per_pbr')
    scheduler.add_job(run_crawl_balance_sheet, 'cron', day_of_week='mon', hour='0', minute='0', second='0', id='run_crawl_balance_sheet')
    scheduler.add_job(run_crawl_income_statement, 'cron', day_of_week='mon', hour='0', minute='0', second='0', id='run_crawl_income_statement')
    scheduler.add_job(run_crawl_cash_flow, 'cron', day_of_week='mon', hour='0', minute='0', second='0', id='run_crawl_cash_flow')
    scheduler.add_job(run_crawl_financial_ratios, 'cron', day_of_week='mon', hour='0', minute='0', second='0', id='run_crawl_financial_ratios')


@app.on_event('shutdown')
async def shutdown():
    scheduler.remove_job('run_crawl_one_minute_stick')
    scheduler.remove_job('run_crawl_five_seconds_exchange')
    scheduler.remove_job('run_crawl_brokers_exchange')
    scheduler.remove_job('run_crawl_tradersum')
    scheduler.remove_job('run_crawl_stock_info')
    scheduler.remove_job('run_crawl_stock_revenue')
    scheduler.remove_job('run_crawl_reinvestment')
    scheduler.remove_job('run_crawl_per_pbr')
    scheduler.remove_job('run_crawl_balance_sheet')
    scheduler.remove_job('run_crawl_income_statement')
    scheduler.remove_job('run_crawl_cash_flow')
    scheduler.remove_job('run_crawl_financial_ratios')


if __name__ == '__main__':
    cmoney_run('籌碼K線')
    uvicorn.run('app:app',
                host='0.0.0.0',
                port=8000,
                log_level='info',
                reload=True)
