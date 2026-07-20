from pathlib import Path
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from utils.synthetic_award_generator import calculate_award_points

AWARD_SCHEMA = StructType(
    [
        StructField("flight_id", StringType(), False),
        StructField("origin_airport", StringType(), True),
        StructField("destination_airport", StringType(), True),
        StructField("departure_datetime", StringType(), True),
        StructField("arrival_datetime", StringType(), True),
        StructField("airline", StringType(), True),
        StructField("cabin_class", StringType(), True),
        StructField("points_required", LongType(), False),
        StructField("program", StringType(), False),
        StructField("award_type", StringType(), False),
        StructField("ingestion_timestamp", StringType(), True),
    ]
)


def get_spark():
    return SparkSession.builder.appName("bronze_award_ingestion").getOrCreate()

def read_flight_data(source_path):
    source = Path(source_path)

    if not source.exists():
        raise FileNotFoundError(f"Source path does not exist: {source}")

    return get_spark().read.parquet(str(source))


def normalize_award_rows(df, award_function=calculate_award_points):
    def build_row(row):
        row_data = row.asDict(recursive=True)
        origin = row_data.get("origin_airport")
        destination = row_data.get("destination_airport")
        airline = row_data.get("airline")
        cabin_class = row_data.get("cabin_class", "economy")
        departure_datetime = row_data.get("departure_datetime")
        arrival_datetime = row_data.get("arrival_datetime")
        ingestion_timestamp = (
            row_data.get("ingestion_timestamp")
            or row_data.get("ingest_ts")
            or row_data.get("ingestion_date")
        )

        return (
            row_data.get("flight_id"),
            origin,
            destination,
            departure_datetime,
            arrival_datetime,
            airline,
            cabin_class,
            int(award_function(origin, destination, airline, cabin_class)),
            "synthetic_generic",
            "points",
            ingestion_timestamp,
        )

    rows = df.rdd.map(build_row)
    return get_spark().createDataFrame(rows, schema=AWARD_SCHEMA)

def main():
    source = "data/silver/cash"
    target = "data/bronze/award"

    flights_df = read_flight_data(source)
    bronze_award_df = normalize_award_rows(flights_df)

    bronze_award_df.write.mode("overwrite").parquet(str(target))


if __name__ == "__main__":
    main()

