from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Data folders
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Elasticsearch
ELASTICSEARCH_HOST = "http://localhost:9200"

FLIGHT_INDEX = "flight_deals"
ANOMALY_INDEX = "flight_anomalies"

# APIs
SKYSCANNER_API_KEY = ""
AMADEUS_API_KEY = ""

# Data files
CASH_DATA_PATH = BRONZE_DIR / "cash"
AWARD_DATA_PATH = BRONZE_DIR / "awards"

# Project constants
DEFAULT_CURRENCY = "USD"

PIPELINE_NAME = "Flight Redemption Analytics Platform"