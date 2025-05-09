from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SendToKafka") \
    .getOrCreate()

df = spark.read.option("recursiveFileLookup", "true").parquet("s3a://trendwatch/processed_data/")

json_df = df.selectExpr("CAST(to_json(struct(*)) AS STRING) AS value")

print("--------------------------DATA FRAME INFO--------------------------\n\n")
print(df.count)
print("\n\n--------------------------DATA FRAME INFO--------------------------")


# Пишем в Kafka-топик
json_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "processed-data") \
    .save()
