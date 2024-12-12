from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from jobs.extract.fetch_data import fetch_and_store
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

api_key = Variable.get("MARKET_CAL_API_KEY")

with DAG(
    "coinmarketcal_events_extract",
    default_args=default_args,
    description="Extract events data from CoinMarketCal API",
    # schedule_interval="*/30 * * * *",
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    coinmarketcal_events_extract = PythonOperator(
        task_id="coinmarketcal_events_extract",
        python_callable=fetch_and_store,
        op_kwargs={
            "api_name": "CoinMarketCalAPI",
            "endpoint": "events",
            "url": "https://developers.coinmarketcal.com/v1/events",
            "bucket_name": "trendwatch",
            "headers": {
                "x-api-key": api_key,
                "Accept-encoding": "deflate, gzip",
                "Accept": "application/json",
            },
            "params": {"page": 1, "max": 16},
        },
    )

    coinmarketcal_events_extract