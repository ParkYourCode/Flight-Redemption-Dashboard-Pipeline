from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder.appName("silver_cash_transformation").getOrCreate()

df = spark.read.parquet("data/bronze/cash/")

df_silver = df.withColumn(
    "departure_ts",
    to_timestamp(col("departure_datetime"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
).withColumn(
    "arrival_ts",
    to_timestamp(col("arrival_datetime"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
).withColumn(
    "flight_duration_minutes",
    (col("arrival_ts").cast("long") - col("departure_ts").cast("long")) / 60
)

df_silver.write.mode("overwrite").parquet("data/silver/cash/")