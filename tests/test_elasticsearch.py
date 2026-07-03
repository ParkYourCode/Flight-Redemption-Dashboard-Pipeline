from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

if not es.indices.exists(index="flight_test"):
    es.indices.create(index="flight_test")

doc = {
    "origin": "LAX",
    "destination": "JFK",
    "airline": "Delta",
    "cash_price": 320,
    "points": 22000,
    "cpp": 1.45
}

es.index(index="flight_test", document=doc)

result = es.search(
    index="flight_test",
    query={
        "match": {
            "origin": "LAX"
        }
    }
)

print(result["hits"]["hits"])