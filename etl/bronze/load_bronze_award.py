import argparse
import importlib.util
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

spark = SparkSession.builder.appName("bronze_award_ingestion").getOrCreate()


def load_synthetic_award_module():
    script_path = Path(__file__).resolve().parents[2] / "utils" / "synthetic_award_generator.py"
    spec = importlib.util.spec_from_file_location("synthetic_award_generator", str(script_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if hasattr(module, "calculate_award_points"):
        return module.calculate_award_points
    for name in ("generate_award_points", "assign_award_points", "award_points_for_flight"):
        if hasattr(module, name):
            return getattr(module, name)
    raise ImportError("No award point generation function found in synthetic_award_generator.py")


def read_flight_data(source_path):
    source = Path(source_path)
    if source.is_dir():
        parquet_files = sorted(source.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {source}")
        return spark.read.parquet(*[str(path) for path in parquet_files])
    if source.is_file() and source.suffix.lower() == ".json":
        return spark.read.option("multiline", True).json(str(source))
    if source.is_file() and source.suffix.lower() == ".csv":
        return spark.read.option("header", True).csv(str(source))
    return spark.read.parquet(str(source))


def build_bronze_schema():
    return StructType([
        StructField("flight_id", StringType(), True),
        StructField("origin_airport", StringType(), True),
        StructField("destination_airport", StringType(), True),
        StructField("departure_datetime", StringType(), True),
        StructField("arrival_datetime", StringType(), True),
        StructField("airline", StringType(), True),
        StructField("cabin_class", StringType(), True),
        StructField("points_required", LongType(), True),
        StructField("program", StringType(), True),
        StructField("award_type", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
    ])


def normalize_award_rows(df, award_function):
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
            int(award_function(origin, destination, airline, cabin_class) or 0),
            "award",
            "points",
            ingestion_timestamp,
        )

    rows = df.rdd.map(build_row)
    return spark.createDataFrame(rows, schema=build_bronze_schema())


def write_bronze_award(df, target_path):
    df.write.mode("overwrite").parquet(str(target_path))


def main():
    parser = argparse.ArgumentParser(description="Load bronze award points for flights")
    parser.add_argument("--source", default=str(Path(__file__).resolve().parents[2] / "data" / "silver" / "cash"))
    parser.add_argument("--target", default=str(Path(__file__).resolve().parents[2] / "data" / "bronze" / "award"))
    args = parser.parse_args()

    award_function = load_synthetic_award_module()
    flights_df = read_flight_data(args.source)
    bronze_award_df = normalize_award_rows(flights_df, award_function)
    write_bronze_award(bronze_award_df, args.target)


if __name__ == "__main__":
    main()

