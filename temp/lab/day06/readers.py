from datasource import DataSource
import csv

class CSVReader(DataSource):
    def __init__(self, path):
        self.path = path

    def read(self):
        with open(self.path, newline='', encoding='utf-8') as f:
            return list(csv.DictReader(f))

class MockDBReader(DataSource):
    def read(self):
        return [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
