import json
from datetime import date, datetime, timedelta, timezone
from config.routes import ROUTES as routes
from clients.ignav_client import get_flights

raw_records = []
departure_date = (date.today() + timedelta(days=7)).isoformat()

def ingest_cash_flights():
    """Ingest cash flights for all defined routes."""
    for route in routes:
        origin = route["origin"]
        destination = route["destination"]

        print(f"Processing route: {origin} -> {destination}")

        response = get_flights(origin, destination, departure_date)

        print(response)

        if response is None:
            print(f"Failed to fetch flights for route: {origin} -> {destination}")
            return

        if not response:
            print(f"No response for route: {origin} -> {destination}")
            continue
        
        itineraries = response.get("itineraries", [])

        print(f"Found {len(itineraries)} itineraries for route: {origin} -> {destination}")

        raw_records.append({
            "origin": origin,
            "destination": destination,
            "departure_date": departure_date,
            "response": response,
            "ingest_ts": datetime.now(timezone.utc).isoformat()
        })

if __name__ == "__main__":
    ingest_cash_flights()

    with open("raw_records.json", "w") as f:
        json.dump(raw_records, f, indent=4)