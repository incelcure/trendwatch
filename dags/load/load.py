from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='send_parquet_to_kafka',
    default_args=default_args,
    description='Load new parquet files from S3 to Kafka',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2025, 5, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    send_to_kafka = SparkSubmitOperator(
        task_id='send_to_kafka_task',
        application='/opt/airflow/jobs/load/load.py',  # путь внутри контейнера
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,'
                'org.apache.hadoop:hadoop-aws:3.3.6,'
                'org.apache.hadoop:hadoop-client-api:3.3.6,org.apache.hadoop:hadoop-client-runtime:3.3.6',
        jars='/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar,/opt/spark/jars/jsr305-3.0.2.jar',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.hadoop.fs.s3a.access.key': 'trend-access-key-id',  # todo: move secrets to airflow env
            'spark.hadoop.fs.s3a.secret.key': 'trend-secret-key',
            'spark.hadoop.fs.s3a.endpoint': 'http://localstack:4566',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'
        },
        executor_cores=1,
        executor_memory='1g',
        name='SendParquetToKafkaJob',
        verbose=True
    )
