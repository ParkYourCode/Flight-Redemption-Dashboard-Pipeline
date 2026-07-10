import os
import sys

import pytest
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from etl.gold.build_gold_analytics import build_gold_analytics


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-gold-analytics")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_build_gold_analytics_calculates_cpp(spark):
    cash_df = spark.createDataFrame(
        [
            (
                "flight-1",
                "LAX",
                "JFK",
                "Delta",
                "2026-07-01T10:00:00Z",
                "2026-07-01T18:00:00Z",
                320.0,
                "USD",
                "2026-07-01T00:00:00Z",
            )
        ],
        [
            "flight_id",
            "origin_airport",
            "destination_airport",
            "airline",
            "departure_datetime",
            "arrival_datetime",
            "cash_price",
            "currency",
            "ingestion_timestamp",
        ],
    )

    award_df = spark.createDataFrame(
        [
            (
                "flight-1",
                "LAX",
                "JFK",
                "2026-07-01T10:00:00Z",
                "2026-07-01T18:00:00Z",
                "Delta",
                "economy",
                32000,
                "award",
                "points",
                "2026-07-01T00:00:00Z",
            )
        ],
        [
            "flight_id",
            "origin_airport",
            "destination_airport",
            "departure_datetime",
            "arrival_datetime",
            "airline",
            "cabin_class",
            "points_required",
            "program",
            "award_type",
            "ingestion_timestamp",
        ],
    )

    gold_df = build_gold_analytics(cash_df, award_df)
    rows = gold_df.collect()

    assert len(rows) == 1
    assert rows[0]["cpp_cents_per_point"] == pytest.approx(1.0)
    assert rows[0]["is_good_redemption"] is True
