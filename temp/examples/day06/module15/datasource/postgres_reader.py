from typing import List, Dict, Any
from .base_db import BaseDataSource


class PostgresReader(BaseDataSource):
    def __init__(self, host: str, port: int, database: str, query: str):
        super().__init__(host, port, database)
        self.query = query

    def connect(self) -> None:
        print(f"[PostgresReader] Initializing Postgres-specific settings.")
        super().connect()
        print(f"[PostgresReader] Running pre-flight checks.")

    def read(self) -> List[Dict[str, Any]]:
        if not self._connection:
            raise RuntimeError("Not connected. Call connect() first.")
        print(f"[PostgresReader] Executing query: {self.query}")
        return [
            {"order_id": "1001", "customer": "Alice", "amount": 250.0},
            {"order_id": "1002", "customer": "Bob",   "amount": 175.5},
        ]
