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

api_key = Variable.get("CRYPTOCOMPARE_API_KEY")

with DAG(
    "cryptocompare_news_articles_extract",
    default_args=default_args,
    description="Extract latest news articles from CryptocompareAPI",
    # schedule_interval="*/30 * * * *",
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    cryptocompare_news_articles_extract = PythonOperator(
        task_id="cryptocompare_news_articles_extract",
        python_callable=fetch_and_store,
        op_kwargs={
            "api_name": "CryptoCompareAPI",
            "endpoint": "news/v1/articles/list",
            "url": "https://data-api.cryptocompare.com/news/v1/article/list",
            "bucket_name": "trendwatch",
            "headers": {
                "x-api-key": api_key,
                "Acharset": "UTF-8",
                "Accept": "application/json",
            },
            "params": {"lang": "EN", "limit": 10},
        },
    )

    cryptocompare_news_articles_extract