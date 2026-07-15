import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from utils.synthetic_award_generator import calculate_award_points

raw_records = []


def read_silver_flights():
    """Read the silver cash parquet file into a dataframe."""
    silver_path = Path(__file__).resolve().parents[1] / "data" / "silver" / "cash" / "part-00000-885e84c1-d652-4677-8647-c8f303df8b8b-c000.snappy.parquet"

    if not silver_path.exists():
        raise FileNotFoundError(f"No parquet file found at {silver_path}")

    return pd.read_parquet(silver_path)


def build_flat_award_record(row):
    """Create a flat award record from a silver-table row."""
    origin = row.get("origin_airport")
    destination = row.get("destination_airport")
    airline = row.get("airline")
    departure_datetime = row.get("departure_datetime")
    arrival_datetime = row.get("arrival_datetime")

    return {
        "flight_id": row.get("flight_id"),
        "origin_airport": origin,
        "destination_airport": destination,
        "departure_datetime": departure_datetime,
        "arrival_datetime": arrival_datetime,
        "airline": airline,
        "cabin_class": "economy",
        "points_required": calculate_award_points(origin, destination, airline, "economy"),
        "program": "award",
        "award_type": "points",
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def ingest_award_flights():
    """Ingest award-flight data from the silver table."""
    silver_df = read_silver_flights()

    for _, row in silver_df.iterrows():
        row_data = row.to_dict()
        origin = row_data.get("origin_airport")
        destination = row_data.get("destination_airport")

        print(f"Processing route: {origin} -> {destination}")

        record = build_flat_award_record(row_data)
        raw_records.append(record)


if __name__ == "__main__":
    ingest_award_flights()

    with open("raw_records.json", "w") as f:
        json.dump(raw_records, f, indent=4)