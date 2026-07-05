import uuid
from datetime import datetime

def extract_cash_flights(raw_records):
    """Extract cash flight data from raw records."""
    
    extracted_data = []

    for record in raw_records:

        response = record.get("response", {})
        origin = record.get("origin")
        destination = record.get("destination")
        ingest_ts = record.get("ingest_ts")

        itineraries = response.get("itineraries", [])

        for it in itineraries:
            ignav_id = it.get("id", str(uuid.uuid4()))
            price = it.get("price", {})
            outbound = it.get("outbound", {})
            airline = outbound.get("carrier")
            segments = outbound.get("segments", [])

            if not segments:
                continue

            first_segment = segments[0]
            last_segment = segments[-1]

            extracted_data.append({
                "flight_id": ignav_id,

                "origin": origin,
                "destination": destination,

                "airline": airline,
                
                "departure_date": first_segment.get("departure_time_utc"),
                "arrival_dt": last_segment.get("arrival_time_utc"),

                "price": price.get("amount"),
                "currency": price.get("currency"),

                "ingest_ts": ingest_ts,
            })

    return extracted_data