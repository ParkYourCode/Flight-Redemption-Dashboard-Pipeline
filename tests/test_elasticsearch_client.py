import pandas as pd

from search.elasticsearch_client import dataframe_to_actions


def test_dataframe_to_actions_serializes_flight_documents():
    flights = pd.DataFrame(
        [
            {
                "flight_id": "flight-1",
                "cash_price": 320.0,
                "departure_datetime": pd.Timestamp("2026-07-01T10:00:00Z"),
                "cpp_cents_per_point": float("nan"),
            }
        ]
    )

    actions = list(dataframe_to_actions(flights, "flight_redemptions"))

    assert actions == [
        {
            "_index": "flight_redemptions",
            "_id": "flight-1",
            "_source": {
                "flight_id": "flight-1",
                "cash_price": 320.0,
                "departure_datetime": "2026-07-01T10:00:00+00:00",
                "cpp_cents_per_point": None,
            },
        }
    ]
