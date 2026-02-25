from datasource.base_db import BaseDataSource


class BaseDataSource(BaseDataSource):
    pass


def run_db_pipeline(reader):
    reader.connect()
    try:
        records = reader.read()
        for record in records:
            print(f"  Record: {record}")
    finally:
        reader.close()


if __name__ == "__main__":
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

    from datasource.base_db import BaseDataSource as _Base
    from datasource.postgres_reader import PostgresReader

    reader = PostgresReader(
        host="db.internal.company.com",
        port=5432,
        database="orders_db",
        query="SELECT order_id, customer, amount FROM orders WHERE status = 'PENDING'",
    )
    run_db_pipeline(reader)
