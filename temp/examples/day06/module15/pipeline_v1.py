"""Bad-design example from the guide (module 15)"""
import csv


def read_csv(filepath):
    records = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    return records


def read_mock_db():
    return [
        {"id": "1", "name": "Alice", "email": "alice@example.com"},
        {"id": "2", "name": "Bob",   "email": "bob@example.com"},
    ]


def run_pipeline(source_type):
    if source_type == "csv":
        records = read_csv("users.csv")
    elif source_type == "db":
        records = read_mock_db()
    else:
        raise ValueError(f"Unknown source: {source_type}")

    for record in records:
        print(f"Processing: {record}")


if __name__ == "__main__":
    run_pipeline("db")
