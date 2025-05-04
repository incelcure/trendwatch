from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SendToKafka") \
    .getOrCreate()

# Читаем все parquet-файлы рекурсивно
df = spark.read.option("recursiveFileLookup", "true").parquet("s3a://trendwatch/processed_data/")

# Преобразуем строки в JSON-объекты
json_df = df.selectExpr("CAST(to_json(struct(*)) AS STRING) AS value")

# Пишем в Kafka-топик
json_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "processed-data") \
    .save()
