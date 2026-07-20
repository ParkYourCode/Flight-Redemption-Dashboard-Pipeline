from pyspark.sql import SparkSession
from pyspark.sql.functions import array_size, col, explode, to_date
from pathlib import Path

spark = SparkSession.builder.appName("bronze_cash_ingestion").getOrCreate()

# Resolve raw_records.json relative to the project root (two levels up from this file)
data_path = Path(__file__).resolve().parents[2] / "raw_records.json"
df_raw = spark.read.option("multiline", "true").json(str(data_path))

df_itin = df_raw.withColumn(
    "itinerary",
    explode(col("response.itineraries"))
)

df_bronze = df_itin.select(
    col("itinerary.ignav_id").alias("flight_id"),

    col("origin").alias("origin_airport"),
    col("destination").alias("destination_airport"),

    col("itinerary.outbound.carrier").alias("airline"),

    col("itinerary.outbound.segments")[0]["departure_time_utc"].alias("departure_datetime"),
    col("itinerary.outbound.segments")[array_size(col("itinerary.outbound.segments")) - 1]["arrival_time_utc"].alias("arrival_datetime"),

    col("itinerary.cabin_class").alias("cabin_class"),
    col("itinerary.price.amount").alias("cash_price"),
    col("itinerary.price.currency").alias("currency"),

    col("ingest_ts").alias("ingestion_timestamp")
)

# derive ingestion_date from ingestion_timestamp and write partitioned by it
df_bronze = df_bronze.withColumn("ingestion_date", to_date(col("ingestion_timestamp")))

df_bronze.write.mode("overwrite").partitionBy("ingestion_date").parquet("data/bronze/cash/")
