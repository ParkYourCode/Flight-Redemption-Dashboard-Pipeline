import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from search.elasticsearch_client import index_gold_parquet


if __name__ == "__main__":
    indexed_count = index_gold_parquet(str(PROJECT_ROOT / "data" / "gold"))
    print(f"Indexed {indexed_count} flight documents in Elasticsearch.")
