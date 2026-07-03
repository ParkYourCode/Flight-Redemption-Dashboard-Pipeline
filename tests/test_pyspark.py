import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FlightRedemptionPlatform")
    .master("local[*]")
    .getOrCreate()
)

data = [
    ("LAX", "JFK", 320),
    ("SFO", "ORD", 280),
    ("SEA", "DFW", 240)
]

columns = ["origin", "destination", "price"]

df = spark.createDataFrame(data, columns)

df.show()

spark.stop()