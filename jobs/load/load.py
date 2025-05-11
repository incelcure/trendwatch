from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_json,
    struct,
    concat_ws,
    concat,
    lit,
    date_format,
    explode,
)

"""
Spark job: reads processed crypto‑event data from S3 and publishes **deduplicated** one‑line news summaries to Kafka.

Kafka message schema (stringified JSON):
    {"timestamp":"2025-05-10T00:00:00Z","summary":"2025‑05‑10 — Toncoin Bridge Retires (TON)"}

Works only with СoinMarketCal events
"""

# ── Spark session ───────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("SendSummaryToKafka")
    .getOrCreate()
)

RAW_PATH = "s3a://trendwatch/processed_data/"
raw_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(RAW_PATH)
)


if "body" in raw_df.columns:
    events_df = raw_df.select(explode("body").alias("event"))
    ev = col("event")
    get = lambda field: ev.getField(field)
else:
    events_df = raw_df.alias("event")
    ev = col("event")
    get = lambda field: ev.getField(field)

base_df = (
    events_df
    .select(
        get("date_event").alias("timestamp"),
        get("title").getField("en").alias("title_en"),
        get("coins")[0].getField("symbol").alias("symbol"),
    )
)

summary_df = (
    base_df
    .select(
        col("timestamp"),
        concat_ws(
            " — ",
            date_format(col("timestamp"), "yyyy-MM-dd"),  # 2025-05-10
            concat(col("title_en"), lit(" ("), col("symbol"), lit(")")),
        ).alias("summary"),
    )
)

json_df = summary_df.select(
    to_json(struct("timestamp", "summary")).alias("value")
)

unique_json_df = json_df.dropDuplicates(["value"])


print("-------------------------- SUMMARY SAMPLE --------------------------")
unique_json_df.show(5, truncate=False)
print("--------------------------------------------------------------------")


(
    unique_json_df.write
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "processed-data")
    .save()
)

spark.stop()
