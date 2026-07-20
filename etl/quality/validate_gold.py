from datetime import datetime, timezone
import os
from pathlib import Path
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    col,
    coalesce,
    concat_ws,
    count,
    explode,
    lit,
    struct,
    sum as spark_sum,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# Canonical contract consumed by both the quality checks and Elasticsearch indexer.
EXPECTED_SCHEMA = {
    "flight_id": "string",
    "origin_airport": "string",
    "destination_airport": "string",
    "airline": "string",
    "departure_datetime": "string",
    "arrival_datetime": "string",
    "cash_price": "double",
    "currency": "string",
    "points_required": "bigint",
    "cpp_cents_per_point": "double",
    "cabin_class": "string",
    "program": "string",
    "award_type": "string",
    "ingestion_timestamp": "string",
}
REQUIRED_FIELDS = tuple(EXPECTED_SCHEMA)
# A field-level warning is emitted only when more than 5% of records are empty.
DEFAULT_NULL_RATE_THRESHOLD = 0.05

FLAG_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("flight_id", StringType(), True),
        StructField("check_name", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("field_name", StringType(), True),
        StructField("message", StringType(), False),
        StructField("observed_value", StringType(), True),
        StructField("detected_at", TimestampType(), False),
    ]
)

METRIC_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("metric_name", StringType(), False),
        StructField("field_name", StringType(), False),
        StructField("metric_value", DoubleType(), False),
        StructField("threshold", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("total_records", LongType(), False),
        StructField("failed_records", LongType(), False),
        StructField("measured_at", TimestampType(), False),
    ]
)


def get_spark() -> SparkSession:
    """Create the Spark session used by the gold quality stage."""
    return SparkSession.builder.appName("gold_data_quality").getOrCreate()


def _empty_flags(spark: SparkSession) -> DataFrame:
    """Return an empty DataFrame that conforms to the quality-flag schema."""
    return spark.range(0).select(
        *(lit(None).cast(field.dataType).alias(field.name) for field in FLAG_SCHEMA.fields)
    )


def _literal_flag(
    spark: SparkSession,
    run_id: str,
    check_name: str,
    severity: str,
    field_name: str,
    message: str,
    observed_value: str | None,
    detected_at: datetime,
) -> DataFrame:
    """Create a single dataset-level quality flag without a Python RDD."""
    return spark.range(1).select(
        lit(run_id).alias("run_id"),
        lit(None).cast("string").alias("flight_id"),
        lit(check_name).alias("check_name"),
        lit(severity).alias("severity"),
        lit(field_name).alias("field_name"),
        lit(message).alias("message"),
        lit(observed_value).cast("string").alias("observed_value"),
        lit(detected_at).cast("timestamp").alias("detected_at"),
    )


def _union_flags(flag_frames: list[DataFrame], spark: SparkSession) -> DataFrame:
    """Combine flag DataFrames while preserving the canonical flag schema."""
    result = _empty_flags(spark)
    for frame in flag_frames:
        result = result.unionByName(frame)
    return result


def find_schema_issues(
    df: DataFrame,
    run_id: str,
    detected_at: datetime,
) -> DataFrame:
    """Flag missing, incorrectly typed, and unexpected gold columns."""
    # Schema checks run before missing columns are added for downstream row checks.
    actual_schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    flag_frames = []

    for field_name, expected_type in EXPECTED_SCHEMA.items():
        actual_type = actual_schema.get(field_name)
        if actual_type is None:
            flag_frames.append(
                _literal_flag(
                    df.sparkSession,
                    run_id,
                    "SCHEMA_MISSING_COLUMN",
                    "error",
                    field_name,
                    f"Required column '{field_name}' is missing.",
                    None,
                    detected_at,
                )
            )
        elif actual_type != expected_type:
            flag_frames.append(
                _literal_flag(
                    df.sparkSession,
                    run_id,
                    "SCHEMA_TYPE_MISMATCH",
                    "error",
                    field_name,
                    f"Expected {expected_type} but received {actual_type}.",
                    actual_type,
                    detected_at,
                )
            )

    for field_name in sorted(set(actual_schema) - set(EXPECTED_SCHEMA)):
        flag_frames.append(
            _literal_flag(
                df.sparkSession,
                run_id,
                "SCHEMA_UNEXPECTED_COLUMN",
                "warning",
                field_name,
                f"Unexpected column '{field_name}' was found.",
                actual_schema[field_name],
                detected_at,
            )
        )

    return _union_flags(flag_frames, df.sparkSession)


def add_missing_columns(df: DataFrame) -> DataFrame:
    """Add absent canonical columns as typed null values for record validation."""
    # Add absent columns as typed nulls so all record checks can still execute and report them.
    normalized = df
    for field_name, expected_type in EXPECTED_SCHEMA.items():
        if field_name not in normalized.columns:
            normalized = normalized.withColumn(field_name, lit(None).cast(expected_type))
    return normalized


def find_record_issues(
    df: DataFrame,
    run_id: str,
    detected_at: datetime,
) -> DataFrame:
    """Flag missing values, duplicate IDs, and invalid flight timestamps."""
    spark = df.sparkSession
    # Build nullable flag structs and explode them once, avoiding one Spark scan per field.
    candidate_flags = []

    for field_name in REQUIRED_FIELDS:
        field = col(field_name)
        condition = field.isNull()
        if EXPECTED_SCHEMA[field_name] == "string":
            condition = condition | (trim(field) == "")
        candidate_flags.append(
            when(
                condition,
                struct(
                    lit(run_id).alias("run_id"),
                    col("flight_id").cast("string").alias("flight_id"),
                    lit("MISSING_REQUIRED_FIELD").alias("check_name"),
                    lit("error").alias("severity"),
                    lit(field_name).alias("field_name"),
                    lit(f"Required field '{field_name}' is empty.").alias("message"),
                    field.cast("string").alias("observed_value"),
                    lit(detected_at).cast("timestamp").alias("detected_at"),
                ),
            )
        )

    # Duplicate detection is aggregated separately because it operates across records.
    duplicate_ids = (
        df.where(col("flight_id").isNotNull())
        .groupBy("flight_id")
        .agg(count(lit(1)).alias("duplicate_count"))
        .where(col("duplicate_count") > 1)
        .select(
            lit(run_id).alias("run_id"),
            col("flight_id").cast("string").alias("flight_id"),
            lit("DUPLICATE_FLIGHT_ID").alias("check_name"),
            lit("error").alias("severity"),
            lit("flight_id").alias("field_name"),
            lit("Flight ID occurs more than once in the gold dataset.").alias("message"),
            col("duplicate_count").cast("string").alias("observed_value"),
            lit(detected_at).cast("timestamp").alias("detected_at"),
        )
    )
    # Parse at validation time while preserving the original timestamp strings as evidence.
    departure_ts = to_timestamp(col("departure_datetime"))
    arrival_ts = to_timestamp(col("arrival_datetime"))
    invalid_times = (
        col("departure_datetime").isNotNull()
        & col("arrival_datetime").isNotNull()
        & (departure_ts.isNull() | arrival_ts.isNull() | (arrival_ts <= departure_ts))
    )
    candidate_flags.append(
        when(
            invalid_times,
            struct(
                lit(run_id).alias("run_id"),
                col("flight_id").cast("string").alias("flight_id"),
                lit("INVALID_FLIGHT_TIMES").alias("check_name"),
                lit("error").alias("severity"),
                lit("departure_datetime,arrival_datetime").alias("field_name"),
                lit("Arrival must be a valid timestamp after departure.").alias("message"),
                concat_ws(" -> ", col("departure_datetime"), col("arrival_datetime")).alias(
                    "observed_value"
                ),
                lit(detected_at).cast("timestamp").alias("detected_at"),
            ),
        )
    )

    row_flags = (
        df.select(explode(array(*candidate_flags)).alias("flag"))
        .where(col("flag").isNotNull())
        .select("flag.*")
    )
    return _union_flags([row_flags, duplicate_ids], spark)


def calculate_null_rate_metrics(
    df: DataFrame,
    run_id: str,
    measured_at: datetime,
    threshold: float = DEFAULT_NULL_RATE_THRESHOLD,
) -> DataFrame:
    """Calculate a null-rate metric and threshold status for each required field."""
    # Calculate every field's missing count in one aggregate pass over the gold dataset.
    aggregations = []
    for field_name in REQUIRED_FIELDS:
        field = col(field_name)
        missing = field.isNull()
        if EXPECTED_SCHEMA[field_name] == "string":
            missing = missing | (trim(field) == "")
        aggregations.append(
            spark_sum(when(missing, 1).otherwise(0)).cast("long").alias(field_name)
        )

    summary = df.agg(count(lit(1)).cast("long").alias("total_records"), *aggregations)
    metric_structs = []
    for field_name in REQUIRED_FIELDS:
        failed_records = coalesce(col(field_name), lit(0).cast("long"))
        null_rate = when(
            col("total_records") > 0,
            failed_records.cast("double") / col("total_records"),
        ).otherwise(lit(0.0))
        metric_structs.append(
            struct(
                lit(run_id).alias("run_id"),
                lit("null_rate").alias("metric_name"),
                lit(field_name).alias("field_name"),
                null_rate.cast("double").alias("metric_value"),
                lit(float(threshold)).alias("threshold"),
                when(null_rate > threshold, lit("failed")).otherwise(lit("passed")).alias(
                    "status"
                ),
                col("total_records").alias("total_records"),
                failed_records.alias("failed_records"),
                lit(measured_at).cast("timestamp").alias("measured_at"),
            )
        )
    return summary.select(explode(array(*metric_structs)).alias("metric")).select("metric.*")


def null_rate_flags(metrics: DataFrame) -> DataFrame:
    """Convert failed null-rate metrics into actionable dataset-level flags."""
    return metrics.where(col("status") == "failed").select(
        col("run_id"),
        lit(None).cast("string").alias("flight_id"),
        lit("NULL_RATE_THRESHOLD_EXCEEDED").alias("check_name"),
        lit("warning").alias("severity"),
        col("field_name"),
        lit("Field null rate exceeded its configured threshold.").alias("message"),
        concat_ws(
            ", ",
            concat_ws("=", lit("rate"), col("metric_value").cast("string")),
            concat_ws("=", lit("threshold"), col("threshold").cast("string")),
        ).alias("observed_value"),
        col("measured_at").alias("detected_at"),
    )


def validate_gold_data(
    df: DataFrame,
    run_id: str | None = None,
    threshold: float = DEFAULT_NULL_RATE_THRESHOLD,
) -> tuple[DataFrame, DataFrame]:
    """Run all gold checks and return record flags plus run-level metrics."""
    detected_at = datetime.now(timezone.utc).replace(tzinfo=None)
    run_id = run_id or detected_at.strftime("%Y%m%dT%H%M%S%fZ")
    schema_flags = find_schema_issues(df, run_id, detected_at)
    # Gold remains immutable here; normalization only makes the validation plan executable.
    normalized = add_missing_columns(df)
    record_flags = find_record_issues(normalized, run_id, detected_at)
    metrics = calculate_null_rate_metrics(normalized, run_id, detected_at, threshold)
    flags = _union_flags(
        [schema_flags, record_flags, null_rate_flags(metrics)],
        df.sparkSession,
    )
    return flags, metrics


def write_validation_results(
    gold_path: str,
    flags_path: str,
    metrics_path: str,
) -> None:
    """Validate gold Parquet data and write flags and metrics as Parquet outputs."""
    spark = get_spark()
    try:
        gold = spark.read.parquet(gold_path)
        flags, metrics = validate_gold_data(gold)
        # Small local outputs use one file each, reducing OneDrive commit overhead.
        flags.coalesce(1).write.mode("overwrite").parquet(flags_path)
        metrics.coalesce(1).write.mode("overwrite").parquet(metrics_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[2]
    write_validation_results(
        str(project_root / "data" / "gold"),
        str(project_root / "data" / "quality" / "flags"),
        str(project_root / "data" / "quality" / "metrics"),
    )
    print("Wrote gold quality flags and null-rate metrics.")
