from typing import List, Dict, Any
from datasource.base import DataSource


def run_pipeline(source: DataSource) -> List[Dict[str, Any]]:
    source.connect()
    try:
        records = source.read()
        print(f"[Pipeline] Loaded {len(records)} records.")
        return records
    finally:
        source.close()


if __name__ == "__main__":
    # Make package imports work when running this script directly
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

    from datasource.csv_reader import CSVReader
    from datasource.mock_db_reader import MockDBReader

    # db source demo
    db_source = MockDBReader(table="users")
    records = run_pipeline(db_source)
    for record in records:
        print(record)
