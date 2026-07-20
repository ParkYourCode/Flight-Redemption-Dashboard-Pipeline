import math
import os
from datetime import date, datetime

import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


DEFAULT_INDEX_NAME = "flight_redemptions"
INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "flight_id": {"type": "keyword"},
            "origin_airport": {"type": "keyword"},
            "destination_airport": {"type": "keyword"},
            "airline": {"type": "keyword"},
            "departure_datetime": {"type": "date"},
            "arrival_datetime": {"type": "date"},
            "cash_price": {"type": "double"},
            "currency": {"type": "keyword"},
            "points_required": {"type": "long"},
            "cpp_cents_per_point": {"type": "double"},
            "cabin_class": {"type": "keyword"},
            "program": {"type": "keyword"},
            "award_type": {"type": "keyword"},
            "ingestion_timestamp": {"type": "date"},
        }
    }
}


def get_client() -> Elasticsearch:
    return Elasticsearch(os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"))


def get_index_name() -> str:
    return os.getenv("ELASTICSEARCH_INDEX", DEFAULT_INDEX_NAME)


def create_flight_index(client: Elasticsearch, index_name: str | None = None) -> str:
    """Recreate the local-development index from the latest gold layer."""
    index_name = index_name or get_index_name()
    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
    client.indices.create(index=index_name, **INDEX_MAPPING)
    return index_name


def _serialize_value(value):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value.item() if hasattr(value, "item") else value


def dataframe_to_actions(flights: pd.DataFrame, index_name: str):
    for flight in flights.to_dict(orient="records"):
        document = {field: _serialize_value(value) for field, value in flight.items()}
        yield {"_index": index_name, "_id": document["flight_id"], "_source": document}


def index_gold_parquet(
    gold_path: str,
    client: Elasticsearch | None = None,
    index_name: str | None = None,
) -> int:
    """Load curated Parquet data and replace the Elasticsearch search index."""
    client = client or get_client()
    index_name = create_flight_index(client, index_name)
    flights = pd.read_parquet(gold_path)
    if flights.empty:
        return 0

    successful, _ = bulk(client, dataframe_to_actions(flights, index_name), refresh="wait_for")
    return successful


def search_flights(
    client: Elasticsearch,
    *,
    origin: str | None = None,
    destination: str | None = None,
    departure_date: date | None = None,
    cabin: str | None = None,
    airline: str | None = None,
    sort_by: str = "cpp",
    size: int = 100,
) -> pd.DataFrame:
    filters = []
    for field, value in {
        "origin_airport": origin,
        "destination_airport": destination,
        "cabin_class": cabin,
        "airline": airline,
    }.items():
        if value and value != "All":
            filters.append({"term": {field: value}})

    if departure_date:
        start = datetime.combine(departure_date, datetime.min.time()).isoformat()
        end = datetime.combine(departure_date, datetime.max.time()).isoformat()
        filters.append({"range": {"departure_datetime": {"gte": start, "lte": end}}})

    sort = [{"cpp_cents_per_point": {"order": "desc", "missing": "_last"}}]
    if sort_by == "cash_price":
        sort = [{"cash_price": {"order": "asc", "missing": "_last"}}]

    response = client.search(
        index=get_index_name(),
        query={"bool": {"filter": filters}},
        sort=sort,
        size=size,
    )
    return pd.DataFrame([hit["_source"] for hit in response["hits"]["hits"]])


def get_filter_options(client: Elasticsearch) -> dict[str, list[str]]:
    aggregations = {
        "origins": {"terms": {"field": "origin_airport", "size": 100}},
        "destinations": {"terms": {"field": "destination_airport", "size": 100}},
        "cabins": {"terms": {"field": "cabin_class", "size": 100}},
        "airlines": {"terms": {"field": "airline", "size": 100}},
    }
    response = client.search(index=get_index_name(), size=0, aggs=aggregations)
    return {
        name: sorted(bucket["key"] for bucket in response["aggregations"][name]["buckets"])
        for name in aggregations
    }
