from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round


spark = SparkSession.builder.appName("gold_analytics").getOrCreate()


def build_gold_analytics(cash_df, award_df):
    cash = cash_df.alias("cash")
    award = award_df.alias("award")

    joined = (
        cash.join(
            award,
            on=[
                "flight_id",
                "origin_airport",
                "destination_airport",
                "airline",
                "departure_datetime",
                "arrival_datetime",
            ],
            how="inner",
        )
        .withColumn("cash_price_cents", col("cash.cash_price") * 100)
        .withColumn("cpp_cents_per_point", round(col("cash_price_cents") / col("award.points_required"), 2))
        .withColumn("is_good_redemption", col("cpp_cents_per_point") <= 2.0)
    )

    return joined.select(
        col("cash.flight_id").alias("flight_id"),
        col("cash.origin_airport").alias("origin_airport"),
        col("cash.destination_airport").alias("destination_airport"),
        col("cash.airline").alias("airline"),
        col("cash.departure_datetime").alias("departure_datetime"),
        col("cash.arrival_datetime").alias("arrival_datetime"),
        col("cash.cash_price").alias("cash_price"),
        col("cash.currency").alias("currency"),
        col("award.points_required").alias("points_required"),
        col("cpp_cents_per_point"),
        col("is_good_redemption"),
        col("award.cabin_class").alias("cabin_class"),
        col("award.program").alias("program"),
        col("award.award_type").alias("award_type"),
        col("cash.ingestion_timestamp").alias("ingestion_timestamp"),
    )


def write_gold_analytics(cash_path, award_path, target_path):
    cash_df = spark.read.parquet(cash_path)
    award_df = spark.read.parquet(award_path)
    gold_df = build_gold_analytics(cash_df, award_df)
    gold_df.write.mode("overwrite").parquet(target_path)


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[2]
    write_gold_analytics(
        str(project_root / "data" / "silver" / "cash"),
        str(project_root / "data" / "bronze" / "award"),
        str(project_root / "data" / "gold"),
    )
