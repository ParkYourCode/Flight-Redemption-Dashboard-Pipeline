from pathlib import Path
from pyspark.sql import SparkSession
from utils.synthetic_award_generator import calculate_award_points

spark = SparkSession.builder.appName("bronze_award_ingestion").getOrCreate()

def read_flight_data(source_path):
    source = Path(source_path)

    if not source.exists():
        raise FileNotFoundError(f"Source path does not exist: {source}")

    if source.is_dir():
        parquet_files = sorted(source.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {source}")
        return spark.read.parquet(*[str(path) for path in parquet_files])

    return spark.read.parquet(str(source))


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
    return spark.createDataFrame(rows)

def main():
    source = "data/silver/cash"
    target = "data/bronze/award"

    award_function = calculate_award_points()
    flights_df = read_flight_data(source)
    bronze_award_df = normalize_award_rows(flights_df, award_function)

    bronze_award_df.write.mode("overwrite").parquet(str(target))


if __name__ == "__main__":
    main()

