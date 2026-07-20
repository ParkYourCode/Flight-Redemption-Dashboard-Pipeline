from datetime import datetime
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession

from etl.quality.validate_gold import (
    calculate_null_rate_metrics,
    find_record_issues,
    find_schema_issues,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-quality")
        .getOrCreate()
    )
    yield session
    session.stop()


def canonical_gold_df(spark):
    return spark.createDataFrame(
        [
            (
                "duplicate-flight",
                "LAX",
                "JFK",
                None,
                "2026-07-01T18:00:00Z",
                "2026-07-01T10:00:00Z",
                320.0,
                "USD",
                32000,
                1.0,
                "economy",
                "synthetic_generic",
                "points",
                "2026-07-01T00:00:00Z",
            ),
            (
                "duplicate-flight",
                "LAX",
                "JFK",
                "Delta",
                "2026-07-01T10:00:00Z",
                "2026-07-01T18:00:00Z",
                320.0,
                "USD",
                32000,
                1.0,
                "economy",
                "synthetic_generic",
                "points",
                "2026-07-01T00:00:00Z",
            ),
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
            "points_required",
            "cpp_cents_per_point",
            "cabin_class",
            "program",
            "award_type",
            "ingestion_timestamp",
        ],
    )


def test_record_checks_find_missing_duplicate_and_invalid_times(spark):
    issues = find_record_issues(
        canonical_gold_df(spark),
        run_id="run-1",
        detected_at=datetime(2026, 7, 1),
    )

    check_names = {row["check_name"] for row in issues.collect()}

    assert "MISSING_REQUIRED_FIELD" in check_names
    assert "DUPLICATE_FLIGHT_ID" in check_names
    assert "INVALID_FLIGHT_TIMES" in check_names


def test_null_rate_metrics_apply_configured_threshold(spark):
    metrics = calculate_null_rate_metrics(
        canonical_gold_df(spark),
        run_id="run-1",
        measured_at=datetime(2026, 7, 1),
        threshold=0.1,
    )

    airline_metric = metrics.where("field_name = 'airline'").collect()[0]

    assert airline_metric["metric_value"] == pytest.approx(0.5)
    assert airline_metric["failed_records"] == 1
    assert airline_metric["status"] == "failed"


def test_schema_checks_find_missing_mismatched_and_extra_columns(spark):
    drifted = spark.createDataFrame(
        [("flight-1", "not-a-number", "extra")],
        ["flight_id", "points_required", "unexpected_field"],
    )

    issues = find_schema_issues(
        drifted,
        run_id="run-1",
        detected_at=datetime(2026, 7, 1),
    ).collect()
    checks_by_field = {(row["check_name"], row["field_name"]) for row in issues}

    assert ("SCHEMA_MISSING_COLUMN", "currency") in checks_by_field
    assert ("SCHEMA_TYPE_MISMATCH", "points_required") in checks_by_field
    assert ("SCHEMA_UNEXPECTED_COLUMN", "unexpected_field") in checks_by_field
