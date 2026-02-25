import csv
from typing import List, Dict, Any
from .base import DataSource


class CSVReader(DataSource):
    def __init__(self, filepath: str):
        self.filepath = filepath
        self._file = None

    def connect(self) -> None:
        print(f"[CSVReader] Opening file: {self.filepath}")
        self._file = open(self.filepath, "r", newline="")

    def read(self) -> List[Dict[str, Any]]:
        if self._file is None:
            raise RuntimeError("Call connect() before read().")
        reader = csv.DictReader(self._file)
        return [dict(row) for row in reader]

    def close(self) -> None:
        if self._file:
            self._file.close()
            print(f"[CSVReader] File closed: {self.filepath}")
