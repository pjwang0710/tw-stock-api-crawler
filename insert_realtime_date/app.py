from prometheus_client import Counter, start_http_server
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from insert_realtime_date.src.crawl_one_minute_stick import run as stick_run
from insert_realtime_date.src.crawl_five_minutes_exchange import run as exchange_run
from prometheus_client import make_wsgi_app
from wsgiref.simple_server import make_server


scheduler = AsyncIOScheduler()

total_one_minute_stick = Counter('crawl_one_minute_k_stick', 'Total one minute k stick count')
total_five_seconds_exchange = Counter('crawl_five_seconds_exchange', 'Total five seconds exchange')


@scheduler.scheduled_job('cron', hour='1-5', minute='*', second=5, id='run_crawl_one_minute_stick')
def run_crawl_one_minute_stick():
    print('start crawling one minute stick...')
    crawl_data_length = stick_run()
    total_one_minute_stick.inc(crawl_data_length)


@scheduler.scheduled_job('cron', hour='1-5', minute='*', second='*/15', id='run_crawl_five_seconds_exchange')
def run_crawl_five_seconds_exchange():
    print('start crawling five seconds exchange...')
    crawl_data_length = exchange_run()
    total_five_seconds_exchange.inc(crawl_data_length)


if __name__ == '__main__':
    app = make_wsgi_app()
    httpd = make_server('', 8000, app)
    httpd.serve_forever()
    # start_http_server(8000)
