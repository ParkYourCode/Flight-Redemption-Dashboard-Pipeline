import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from elasticsearch import ConnectionError as ElasticsearchConnectionError
from elasticsearch import NotFoundError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from search.elasticsearch_client import get_client, get_filter_options, search_flights


st.set_page_config(page_title="Flight Redemption Platform", page_icon="Flight", layout="wide")

GOLD_DATA_PATH = PROJECT_ROOT / "data" / "gold"


@st.cache_resource
def get_search_client():
    client = get_client()
    try:
        client.info()
    except ElasticsearchConnectionError:
        return None
    return client


@st.cache_data(show_spinner="Loading flight data…")
def load_flights(data_path: str, modified_at: float) -> pd.DataFrame:
    del modified_at
    flights = pd.read_parquet(data_path)
    flights["departure_datetime"] = pd.to_datetime(flights["departure_datetime"], utc=True)
    flights["arrival_datetime"] = pd.to_datetime(flights["arrival_datetime"], utc=True)
    return flights


def filter_parquet_flights(
    flights: pd.DataFrame,
    origin: str,
    destination: str,
    departure_date,
    cabin: str,
    airline: str,
    sort_by: str,
) -> pd.DataFrame:
    filtered = flights.copy()
    for field, value in {
        "origin_airport": origin,
        "destination_airport": destination,
        "cabin_class": cabin,
        "airline": airline,
    }.items():
        if value != "All":
            filtered = filtered[filtered[field] == value]
    if departure_date:
        filtered = filtered[filtered["departure_datetime"].dt.date == departure_date]
    return filtered.sort_values(
        "cpp_cents_per_point" if sort_by == "cpp" else "cash_price",
        ascending=sort_by != "cpp",
        na_position="last",
    )


def format_results(results: pd.DataFrame) -> pd.DataFrame:
    if results.empty:
        return results
    results = results.copy()
    results["departure_datetime"] = pd.to_datetime(results["departure_datetime"], utc=True)
    results["arrival_datetime"] = pd.to_datetime(results["arrival_datetime"], utc=True)
    results["Departure"] = results["departure_datetime"].dt.strftime("%Y-%m-%d %H:%M UTC")
    results["Arrival"] = results["arrival_datetime"].dt.strftime("%Y-%m-%d %H:%M UTC")
    results["Cash price"] = results.apply(
        lambda row: f"{row['currency']} {row['cash_price']:,.2f}", axis=1
    )
    results["CPP"] = results["cpp_cents_per_point"].map(
        lambda value: f"{value:.2f} cents" if pd.notna(value) else "Not available"
    )
    return results.rename(
        columns={
            "origin_airport": "Origin",
            "destination_airport": "Destination",
            "airline": "Airline",
            "cabin_class": "Cabin",
            "program": "Award program",
            "points_required": "Points",
        }
    )


st.title("Flight Redemption Platform")
st.caption("Cash fares paired with synthetic award-point estimates. CPP is shown for comparison only.")

client = get_search_client()
search_options = None
if client:
    try:
        search_options = get_filter_options(client)
        st.success("Searching the Elasticsearch index.")
    except NotFoundError:
        st.warning("Elasticsearch is running but has not been indexed. Showing Parquet data instead.")
        client = None

if client is None:
    if not GOLD_DATA_PATH.exists():
        st.info("No curated flight data is available. Run the pipeline after starting Elasticsearch.")
        st.stop()
    flights = load_flights(str(GOLD_DATA_PATH), GOLD_DATA_PATH.stat().st_mtime)
    if flights.empty:
        st.info("The curated flight dataset is empty.")
        st.stop()
    search_options = {
        "origins": sorted(flights["origin_airport"].dropna().unique()),
        "destinations": sorted(flights["destination_airport"].dropna().unique()),
        "cabins": sorted(flights["cabin_class"].dropna().unique()),
        "airlines": sorted(flights["airline"].dropna().unique()),
    }
    st.info("Elasticsearch is unavailable; using the local Parquet fallback.")

with st.sidebar:
    st.header("Search flights")
    origin = st.selectbox("Origin", ["All"] + search_options["origins"])
    destination = st.selectbox("Destination", ["All"] + search_options["destinations"])
    departure_date = st.date_input("Departure date", value=None)
    cabin = st.selectbox("Cabin", ["All"] + search_options["cabins"])
    airline = st.selectbox("Airline", ["All"] + search_options["airlines"])
    sort_label = st.selectbox("Sort by", ["CPP (high to low)", "Cash price (low to high)"])

sort_by = "cpp" if sort_label.startswith("CPP") else "cash_price"
if client:
    results = search_flights(
        client,
        origin=origin,
        destination=destination,
        departure_date=departure_date,
        cabin=cabin,
        airline=airline,
        sort_by=sort_by,
    )
else:
    results = filter_parquet_flights(
        flights, origin, destination, departure_date, cabin, airline, sort_by
    )

st.subheader(f"{len(results):,} matching flights")
if results.empty:
    st.warning("No flights match those filters.")
    st.stop()

display = format_results(results)
st.dataframe(
    display[
        [
            "Origin",
            "Destination",
            "Departure",
            "Arrival",
            "Airline",
            "Cabin",
            "Cash price",
            "Points",
            "CPP",
            "Award program",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)
