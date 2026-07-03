import duckdb

conn = duckdb.connect("flights.db")

conn.execute("""
CREATE TABLE IF NOT EXISTS flights (
    id INTEGER,
    origin VARCHAR,
    destination VARCHAR,
    price DOUBLE
)
""")

conn.execute("""
INSERT INTO flights VALUES
(1, 'LAX', 'JFK', 325.50),
(2, 'SFO', 'ORD', 198.75)
""")

results = conn.execute("""
SELECT * FROM flights
""").fetchall()

print(results)

conn.close()