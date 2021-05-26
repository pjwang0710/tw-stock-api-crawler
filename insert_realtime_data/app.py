from fastapi import FastAPI
import uvicorn
from starlette_exporter import PrometheusMiddleware, handle_metrics
from prometheus_client import Counter
import datetime
import pytz
from prometheus_client.core import REGISTRY
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from insert_realtime_date.src.config import settings
from insert_realtime_date.src.crawl_one_minute_stick import run as stick_run
from insert_realtime_date.src.crawl_five_minutes_exchange import run as exchange_run
from insert_realtime_date.src.crawl_brokers import run as broker_exchange_run

tpe = pytz.timezone('Asia/Taipei')


scheduler = AsyncIOScheduler()

collectors = list(REGISTRY._collector_to_names.keys())
for collector in collectors:
    REGISTRY.unregister(collector)

total_one_minute_stick = Counter('crawl_one_minute_k_stick', 'Total one minute k stick count')
total_five_seconds_exchange = Counter('crawl_five_seconds_exchange', 'Total five seconds exchange')
total_brokers_exchange = Counter('crawl_brokers', 'Total brokers')


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


@app.get('/')
async def home():
    return {'msg': 'Hello World'}


@app.on_event('startup')
async def startup():
    scheduler.start()
    scheduler.add_job(run_crawl_one_minute_stick, 'cron', hour='1-5', minute='*', second=5, id='run_crawl_one_minute_stick')
    scheduler.add_job(run_crawl_five_seconds_exchange, 'cron', hour='1-5', minute='*', second='*/15', id='run_crawl_five_seconds_exchange')
    scheduler.add_job(run_broker_exchange, 'cron', hour='17', minute='0', second='0', id='run_crawl_brokers_exchange')


@app.on_event('shutdown')
async def shutdown():
    scheduler.remove_job('run_crawl_one_minute_stick')
    scheduler.remove_job('run_crawl_five_seconds_exchange')
    scheduler.remove_job('run_crawl_brokers_exchange')


if __name__ == '__main__':
    uvicorn.run('app:app',
                host='0.0.0.0',
                port=8000,
                log_level='info',
                reload=True)
