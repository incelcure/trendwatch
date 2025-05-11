from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'coinmarketcal_events_json_to_parquet',
    default_args=default_args,
    description='Run Spark job to transform CoinmarketcalAPI/events S3 JSON to Parquet',
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2024, 12, 8),
    catchup=False,
)

spark_coinmarketcal_events_submit_task = SparkSubmitOperator(
    task_id='coinmarketcal_events_json_to_parquet',
    application='/opt/airflow/jobs/transform/coinmarketcal/events/transform.py',
    conn_id="spark_default",
    jars='/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar,/opt/spark/jars/jsr305-3.0.2.jar',
    packages='org.apache.hadoop:hadoop-aws:3.3.6,'
             'org.apache.hadoop:hadoop-client-api:3.3.6,org.apache.hadoop:hadoop-client-runtime:3.3.6',
    exclude_packages='com.amazonaws:aws-java-sdk-bundle,',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.hadoop.fs.s3a.access.key': 'trend-access-key-id', # todo: move secrets to airflow env
        'spark.hadoop.fs.s3a.secret.key': 'trend-secret-key',
        'spark.hadoop.fs.s3a.endpoint': 'http://localstack:4566',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'
    },
    dag=dag,
)