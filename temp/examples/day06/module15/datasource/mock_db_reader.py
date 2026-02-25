from typing import List, Dict, Any
from .base import DataSource


class MockDBReader(DataSource):
    def __init__(self, table: str):
        self.table = table
        self._connected = False

    def connect(self) -> None:
        print(f"[MockDBReader] Connecting to mock DB, table: {self.table}")
        self._connected = True

    def read(self) -> List[Dict[str, Any]]:
        if not self._connected:
            raise RuntimeError("Call connect() before read().")
        return [
            {"id": "1", "name": "Alice", "email": "alice@example.com"},
            {"id": "2", "name": "Bob",   "email": "bob@example.com"},
            {"id": "3", "name": "Carol", "email": "carol@example.com"},
        ]

    def close(self) -> None:
        self._connected = False
        print(f"[MockDBReader] Connection closed for table: {self.table}")
