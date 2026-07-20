import pytest
from pyspark.sql import SparkSession

from etl.bronze.load_bronze_award import normalize_award_rows, read_flight_data


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[1]").appName("test-bronze-award-loader").getOrCreate()
    )
    yield spark
    spark.stop()


def test_read_flight_data_rejects_missing_paths(tmp_path):
    missing_path = tmp_path / "missing.parquet"

    with pytest.raises(FileNotFoundError):
        read_flight_data(str(missing_path))


def test_normalize_award_rows_uses_canonical_schema(spark):
    cash_df = spark.createDataFrame(
        [
            (
                "flight-1",
                "LAX",
                "JFK",
                "Delta",
                "economy",
                "2026-07-01T10:00:00Z",
                "2026-07-01T18:00:00Z",
                "2026-07-01T00:00:00Z",
            )
        ],
        [
            "flight_id",
            "origin_airport",
            "destination_airport",
            "airline",
            "cabin_class",
            "departure_datetime",
            "arrival_datetime",
            "ingestion_timestamp",
        ],
    )

    awards = normalize_award_rows(cash_df)
    row = awards.collect()[0]

    assert awards.columns == [
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
    ]
    assert row["points_required"] > 0
    assert row["program"] == "synthetic_generic"
