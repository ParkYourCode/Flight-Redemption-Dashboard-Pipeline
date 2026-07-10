from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

CASH_DATA_PATH = BRONZE_DIR / "cash"
AWARD_DATA_PATH = BRONZE_DIR / "award"

DEFAULT_CURRENCY = "USD"
PIPELINE_NAME = "Flight Redemption Analytics Platform"