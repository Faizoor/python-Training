import time
from typing import List, Dict, Any


class BaseDataSource:
    def __init__(self, host: str, port: int, database: str):
        self.host = host
        self.port = port
        self.database = database
        self._connection = None

    def connect(self) -> None:
        print(f"[BaseDataSource] Connecting to {self.host}:{self.port}/{self.database}")
        time.sleep(0.01)
        self._connection = f"mock_conn::{self.host}::{self.database}"
        print(f"[BaseDataSource] Connected.")

    def read(self) -> List[Dict[str, Any]]:
        raise NotImplementedError("Subclasses must implement read()")

    def close(self) -> None:
        if self._connection:
            print(f"[BaseDataSource] Closing connection: {self._connection}")
            self._connection = None
