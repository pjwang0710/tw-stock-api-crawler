from fastapi import FastAPI
import uvicorn
from starlette_exporter import PrometheusMiddleware, handle_metrics
from prometheus_client import Counter
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from insert_realtime_date.src.config import settings
from insert_realtime_date.src.crawl_one_minute_stick import run as stick_run
from insert_realtime_date.src.crawl_five_minutes_exchange import run as exchange_run

scheduler = AsyncIOScheduler()

app = FastAPI(
    title=settings.PROJECT_NAME, openapi_url=f'{settings.API_V1_STR}/openapi.json'
)


def get_application():
    _app = FastAPI(title=settings.PROJECT_NAME)

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


@app.get('/')
async def home():
    return {'msg': 'Hello World'}


@app.on_event('startup')
async def startup():
    scheduler.start()
    scheduler.add_job(run_crawl_one_minute_stick, 'cron', hour='9-13', minute='*', second=5, id='run_crawl_one_minute_stick')
    scheduler.add_job(run_crawl_five_seconds_exchange, 'cron', hour='9-13', minute='*', second='*/15', id='run_crawl_five_seconds_exchange')


@app.on_event('shutdown')
async def shutdown():
    scheduler.remove_job('run_crawl_one_minute_stick')
    scheduler.remove_job('run_crawl_five_seconds_exchange')


if __name__ == '__main__':
    global total_one_minute_stick, total_five_seconds_exchange
    total_one_minute_stick = Counter('crawl_one_minute_k_stick', 'Total one minute k stick count')
    total_five_seconds_exchange = Counter('crawl_five_seconds_exchange', 'Total five seconds exchange')
    uvicorn.run('app:app',
                host='0.0.0.0',
                port=8000,
                log_level='info',
                reload=True)
