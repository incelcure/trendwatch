from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("S3 JSON to Parquet") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

bucket = "trendwatch"
raw_data_prefix = "raw_data/CoinMarketCalAPI/events/"
processed_data_prefix = "processed_data/CoinMarketCalAPI/events/"

now = datetime.utcnow()
process_interval = timedelta(minutes=2)
start_time = now - process_interval

date_path = start_time.strftime("%Y/%m/%d/%H")
s3_path = f"s3a://{bucket}/{raw_data_prefix}{date_path}/*"

try:
    raw_data_df = spark.read.json(s3_path)
    print(f"Successfully read data from {s3_path}")
    raw_data_df.show(10)

    data_without_source = raw_data_df.drop("source")

    unique_data_df = data_without_source.dropDuplicates()

    transformed_data_df = unique_data_df.withColumn("processed_time", current_timestamp())

    output_path = f"s3a://{bucket}/{processed_data_prefix}{date_path}/"
    transformed_data_df.write.mode("append").parquet(output_path)

    print(f"Data written to {output_path}")
except Exception as e:
    print(f"No new files or error reading data: {e}")

spark.stop()


