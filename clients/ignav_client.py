import requests

API_KEY = "ignav_JdRWYwSEULmIsrVENcTDfA3phUBzlUAH"

BASE_URL = "https://ignav.com/api/fares/one-way"

def get_flights(origin, destination, departure_date):
    try:
        print(f"Fetching {origin} -> {destination}")

        response = requests.post(
            BASE_URL,
            headers={"X-Api-Key": API_KEY},
            json={
                "origin": origin,
                "destination": destination,
                "departure_date": departure_date
            },
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching flights from {origin} to {destination}: {e}")
        return None