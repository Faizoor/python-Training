# Day 06 — Demo Guide
## Advanced Python for Data Engineering

---

**Modules Covered:**
- Module 15: Advanced OOP for Pipelines
- Module 16: Iterators, Generators & Lazy Processing
- Module 17: Config-Driven & Modular Design

---

# Module 15: Advanced OOP for Pipelines

## Scenario

You are building a data ingestion platform that must read from CSV files, databases, and REST APIs. Six engineers are each writing their own `read_csv()`, `read_db()`, and `read_api()` functions. There is no contract, no shared interface, and no way to swap sources without rewriting the pipeline runner.

This module demonstrates how to apply Abstract Base Classes, inheritance, and entity modeling to produce a maintainable, extensible pipeline architecture.

---

## 15.1 — The Problem: Unstructured Function-Based Design

### What This Looks Like in Practice

Before applying OOP patterns, most pipeline code looks like this:

```python
# pipeline_v1.py  — BAD DESIGN (do not follow in production)

import csv

def read_csv(filepath):
    records = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    return records


def read_mock_db():
    # Simulates a DB fetch — hardcoded for now
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


run_pipeline("db")
```

### Why This Breaks at Scale

- Each new data source requires modifying `run_pipeline()`.
- No enforced interface — contributors implement differently.
- Impossible to test sources in isolation without touching the pipeline.
- Cannot swap sources without code changes (violates Open/Closed Principle).
- No type safety or contract enforcement.

---

## 15.2 — Introducing Abstract Base Classes

### What an Abstract Base Class Does

An Abstract Base Class (ABC) defines a **contract** — a set of methods that any subclass **must** implement. It cannot be instantiated directly. This enforces consistency across all pipeline readers.

### Step 1: Define the Abstract Contract

```python
# datasource/base.py

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class DataSource(ABC):
    """
    Abstract contract for all pipeline data sources.
    Any class that reads data into the pipeline must implement this interface.
    """

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the source."""
        pass

    @abstractmethod
    def read(self) -> List[Dict[str, Any]]:
        """Read and return records as a list of dictionaries."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Release any open connections or file handles."""
        pass
```

### Step 2: Implement a CSV Reader

```python
# datasource/csv_reader.py

import csv
from typing import List, Dict, Any
from datasource.base import DataSource


class CSVReader(DataSource):
    """
    Reads records from a CSV file.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self._file_handle = None

    def connect(self) -> None:
        print(f"[CSVReader] Opening file: {self.filepath}")
        self._file_handle = open(self.filepath, "r", newline="")

    def read(self) -> List[Dict[str, Any]]:
        if self._file_handle is None:
            raise RuntimeError("Call connect() before read().")
        reader = csv.DictReader(self._file_handle)
        return [dict(row) for row in reader]

    def close(self) -> None:
        if self._file_handle:
            self._file_handle.close()
            print(f"[CSVReader] File closed: {self.filepath}")
```

### Step 3: Implement a Mock Database Reader

```python
# datasource/mock_db_reader.py

from typing import List, Dict, Any
from datasource.base import DataSource


class MockDBReader(DataSource):
    """
    Simulates reading records from a relational database.
    In production, this would use SQLAlchemy or psycopg2.
    """

    def __init__(self, table: str):
        self.table = table
        self._connected = False

    def connect(self) -> None:
        print(f"[MockDBReader] Connecting to mock DB, table: {self.table}")
        self._connected = True

    def read(self) -> List[Dict[str, Any]]:
        if not self._connected:
            raise RuntimeError("Call connect() before read().")
        # Simulated data — replace with actual DB query in production
        return [
            {"id": "1", "name": "Alice", "email": "alice@example.com"},
            {"id": "2", "name": "Bob",   "email": "bob@example.com"},
            {"id": "3", "name": "Carol", "email": "carol@example.com"},
        ]

    def close(self) -> None:
        self._connected = False
        print(f"[MockDBReader] Connection closed for table: {self.table}")
```

### Step 4: Polymorphic Pipeline Runner

The pipeline runner does not care which source it receives — it only cares that the source follows the contract.

```python
# pipeline_runner.py

from datasource.base import DataSource
from typing import List, Dict, Any


def run_pipeline(source: DataSource) -> List[Dict[str, Any]]:
    """
    Executes the ingestion phase of a pipeline.
    Works with any DataSource implementation.
    """
    source.connect()
    try:
        records = source.read()
        print(f"[Pipeline] Loaded {len(records)} records.")
        return records
    finally:
        source.close()


# --- Entry point ---

if __name__ == "__main__":
    from datasource.csv_reader import CSVReader
    from datasource.mock_db_reader import MockDBReader

    # Run with CSV source
    # csv_source = CSVReader("data/users.csv")
    # records = run_pipeline(csv_source)

    # Run with DB source — same pipeline, different source
    db_source = MockDBReader(table="users")
    records = run_pipeline(db_source)

    for record in records:
        print(record)
```

### Step 5: Demonstrating Enforcement of the Contract

What happens if someone forgets to implement an abstract method?

```python
# demo_incomplete_class.py

from datasource.base import DataSource


class BrokenReader(DataSource):
    """
    This class does not implement all required methods.
    Python will refuse to instantiate it.
    """

    def connect(self):
        pass

    def read(self):
        return []

    # close() is missing — intentionally


# Attempting to instantiate this will raise:
# TypeError: Can't instantiate abstract class BrokenReader
# with abstract method close

try:
    reader = BrokenReader()
except TypeError as e:
    print(f"Caught expected error: {e}")
```

**Output:**
```
Caught expected error: Can't instantiate abstract class BrokenReader with abstract method close
```

Python enforces the contract at instantiation time. This is the cornerstone of ABC-based design.

### Folder Structure

```
project/
    datasource/
        __init__.py
        base.py
        csv_reader.py
        mock_db_reader.py
    pipeline_runner.py
    data/
        users.csv
```

---

## 15.3 — Inheritance and Method Overriding

### Scenario

Your pipeline reads from multiple databases — Postgres, MySQL, and Redshift. Each requires a slightly different connection strategy, but the overall lifecycle (`connect`, `read`, `close`) is the same.

Use a `BaseDataSource` to share common behavior and let subclasses override only what differs.

### Step 1: Base Class with Default Behavior

```python
# datasource/base_db.py

import time
from typing import List, Dict, Any


class BaseDataSource:
    """
    Concrete base class with shared behavior for all DB-type sources.
    Not abstract — provides sensible defaults.
    """

    def __init__(self, host: str, port: int, database: str):
        self.host = host
        self.port = port
        self.database = database
        self._connection = None

    def connect(self) -> None:
        print(f"[BaseDataSource] Connecting to {self.host}:{self.port}/{self.database}")
        time.sleep(0.1)  # Simulate connection latency
        self._connection = f"mock_conn::{self.host}::{self.database}"
        print(f"[BaseDataSource] Connected.")

    def read(self) -> List[Dict[str, Any]]:
        raise NotImplementedError("Subclasses must implement read()")

    def close(self) -> None:
        if self._connection:
            print(f"[BaseDataSource] Closing connection: {self._connection}")
            self._connection = None
```

### Step 2: Extend into a Specific DB Reader

```python
# datasource/postgres_reader.py

from datasource.base_db import BaseDataSource
from typing import List, Dict, Any


class PostgresReader(BaseDataSource):
    """
    Reads data from a PostgreSQL database.
    Extends BaseDataSource — reuses connect() and close().
    Overrides read() with Postgres-specific logic.
    """

    def __init__(self, host: str, port: int, database: str, query: str):
        super().__init__(host, port, database)
        self.query = query

    def connect(self) -> None:
        print(f"[PostgresReader] Initializing Postgres-specific settings.")
        super().connect()  # Delegates connection logic to base class
        print(f"[PostgresReader] Running pre-flight checks.")

    def read(self) -> List[Dict[str, Any]]:
        if not self._connection:
            raise RuntimeError("Not connected. Call connect() first.")
        print(f"[PostgresReader] Executing query: {self.query}")
        # Simulated result — replace with psycopg2 cursor in production
        return [
            {"order_id": "1001", "customer": "Alice", "amount": 250.0},
            {"order_id": "1002", "customer": "Bob",   "amount": 175.5},
        ]
```

### Step 3: Full Execution Demonstration

```python
# demo_inheritance.py

from datasource.postgres_reader import PostgresReader


def run_db_pipeline(reader: PostgresReader):
    reader.connect()
    try:
        records = reader.read()
        for record in records:
            print(f"  Record: {record}")
    finally:
        reader.close()


if __name__ == "__main__":
    reader = PostgresReader(
        host="db.internal.company.com",
        port=5432,
        database="orders_db",
        query="SELECT order_id, customer, amount FROM orders WHERE status = 'PENDING'"
    )
    run_db_pipeline(reader)
```

**Output:**
```
[PostgresReader] Initializing Postgres-specific settings.
[BaseDataSource] Connecting to db.internal.company.com:5432/orders_db
[BaseDataSource] Connected.
[PostgresReader] Running pre-flight checks.
[PostgresReader] Executing query: SELECT order_id, customer, amount FROM orders WHERE status = 'PENDING'
  Record: {'order_id': '1001', 'customer': 'Alice', 'amount': 250.0}
  Record: {'order_id': '1002', 'customer': 'Bob', 'amount': 175.5}
[BaseDataSource] Closing connection: mock_conn::db.internal.company.com::orders_db
```

### What super() Does Here

`super().connect()` calls the parent's `connect()` method without duplicating its code. The subclass adds behavior **around** it — pre-checks before, post-checks after — while the base class owns the core connection logic. This is **controlled extension**.

---

## 15.4 — Entity Modeling in ETL

### Scenario

Your pipeline processes user records from multiple sources. Currently, records are plain dictionaries. Transformations are scattered across functions — validation logic is duplicated, field access is unreliable, and you cannot add behavior to a `dict`.

### Step 1: Dictionary-Based Processing (Before)

```python
# entity_v1_bad.py — BEFORE (do not follow in production)

def transform_user(record: dict) -> dict:
    # Validation duplicated everywhere it is called
    if not record.get("email"):
        raise ValueError("Missing email")
    if not record.get("name"):
        raise ValueError("Missing name")

    return {
        "id":    record["id"],
        "name":  record["name"].strip().title(),
        "email": record["email"].strip().lower(),
        "domain": record["email"].split("@")[-1]
    }


raw_records = [
    {"id": "1", "name": "  alice ", "email": "  ALICE@Example.COM"},
    {"id": "2", "name": "bob",      "email": "bob@company.org"},
    {"id": "3", "name": "",         "email": ""},  # Bad record
]

for raw in raw_records:
    try:
        result = transform_user(raw)
        print(result)
    except ValueError as e:
        print(f"Skipping record {raw.get('id')}: {e}")
```

This works for a single use — but the validation is free-floating and must be copied to every function that handles users.

### Step 2: Introduce the Entity Class (Refactored)

```python
# entities/user.py

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class User:
    """
    Represents a validated, normalized user record in the pipeline.
    Validation and transformation logic lives inside the entity.
    """
    id:     str
    name:   str
    email:  str
    domain: str = field(init=False)

    def __post_init__(self):
        self._validate()
        self._normalize()

    def _validate(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError(f"User {self.id}: name is required.")
        if not self.email or "@" not in self.email:
            raise ValueError(f"User {self.id}: invalid email '{self.email}'.")

    def _normalize(self) -> None:
        self.name  = self.name.strip().title()
        self.email = self.email.strip().lower()
        self.domain = self.email.split("@")[-1]

    @classmethod
    def from_dict(cls, record: dict) -> "User":
        """
        Factory method — constructs User from a raw dictionary record.
        Use this as the single entry point from any data source.
        """
        return cls(
            id=record.get("id", ""),
            name=record.get("name", ""),
            email=record.get("email", ""),
        )

    def to_dict(self) -> dict:
        return {
            "id":     self.id,
            "name":   self.name,
            "email":  self.email,
            "domain": self.domain,
        }
```

### Step 3: Use the Entity in the Pipeline

```python
# pipeline_with_entities.py

from entities.user import User
from typing import List, Dict, Any


def load_raw_records() -> List[Dict[str, Any]]:
    """Simulates loading raw records from any source."""
    return [
        {"id": "1", "name": "  alice ",  "email": "  ALICE@Example.COM"},
        {"id": "2", "name": "bob",        "email": "bob@company.org"},
        {"id": "3", "name": "carol",      "email": "carol@data.io"},
        {"id": "4", "name": "",           "email": ""},        # Invalid
        {"id": "5", "name": "dave",       "email": "notanemail"},  # Invalid
    ]


def build_user_entities(raw_records: List[Dict[str, Any]]) -> List[User]:
    """
    Converts raw dictionaries into validated User entities.
    Invalid records are logged and skipped.
    """
    users = []
    for record in raw_records:
        try:
            user = User.from_dict(record)
            users.append(user)
        except ValueError as e:
            print(f"[WARN] Invalid record skipped: {e}")
    return users


def group_by_domain(users: List[User]) -> Dict[str, List[User]]:
    """Groups User entities by email domain."""
    grouped: Dict[str, List[User]] = {}
    for user in users:
        grouped.setdefault(user.domain, []).append(user)
    return grouped


if __name__ == "__main__":
    raw = load_raw_records()
    users = build_user_entities(raw)

    print(f"\n[Pipeline] Valid users: {len(users)}")
    for user in users:
        print(f"  {user.to_dict()}")

    print("\n[Pipeline] Grouped by domain:")
    grouped = group_by_domain(users)
    for domain, members in grouped.items():
        print(f"  {domain}: {[u.name for u in members]}")
```

**Output:**
```
[WARN] Invalid record skipped: User 4: name is required.
[WARN] Invalid record skipped: User 5: invalid email 'notanemail'.

[Pipeline] Valid users: 3
  {'id': '1', 'name': 'Alice', 'email': 'alice@example.com', 'domain': 'example.com'}
  {'id': '2', 'name': 'Bob',   'email': 'bob@company.org',   'domain': 'company.org'}
  {'id': '3', 'name': 'Carol', 'email': 'carol@data.io',     'domain': 'data.io'}

[Pipeline] Grouped by domain:
  example.com: ['Alice']
  company.org: ['Bob']
  data.io: ['Carol']
```

### Why Entity Modeling Improves the Pipeline

| Concern              | Dict-based                   | Entity-based                        |
|----------------------|------------------------------|-------------------------------------|
| Validation           | Scattered in every function  | Centralized in `__post_init__`      |
| Normalization        | Manual, repeated everywhere  | Automatic on construction           |
| Field access         | `record.get("email", "")` — fragile | `user.email` — explicit          |
| Adding behavior      | Impossible                   | Methods on the class                |
| Readability          | `dict` of unknown shape      | Typed, documented structure         |

---

# Module 16: Iterators, Generators & Lazy Processing

## Scenario

Your pipeline processes log files that are 10GB+ in size. The naive approach loads everything into memory before processing begins. On a 16GB server, this causes OOM errors at 3AM. This module demonstrates how iterators and generators enable true streaming — processing records one at a time, without loading the full dataset.

---

## 16.1 — The Memory Problem

### Demonstration: What Happens with readlines()

```python
# memory_problem_demo.py

import os
import sys

# Create a sample log file for demonstration
def create_sample_log(filepath: str, num_lines: int) -> None:
    with open(filepath, "w") as f:
        for i in range(num_lines):
            status = "ERROR" if i % 100 == 0 else "INFO"
            f.write(f"2024-01-15 10:{i%60:02d}:00 | {status} | Record processed: id={i}\n")


LOGFILE = "/tmp/demo_pipeline.log"
create_sample_log(LOGFILE, 500_000)

# BAD APPROACH — loads all lines into memory at once
def count_errors_naive(filepath: str) -> int:
    with open(filepath, "r") as f:
        lines = f.readlines()          # ENTIRE FILE loaded into memory
    errors = [l for l in lines if "ERROR" in l]
    return len(errors)


# Measure memory impact
before = sys.getsizeof([])
result = count_errors_naive(LOGFILE)
print(f"Error count: {result}")
print(f"Note: all 500,000 lines were in RAM simultaneously.")
print(f"For a 10GB log file, this means 10GB+ of RAM usage before any processing occurs.")
```

**The problem:** `readlines()` materializes the entire file in memory. For large files, this exhausts available RAM before a single record is processed.

---

## 16.2 — Manual Iterator Implementation

### Understanding __iter__ and __next__

A manual iterator class gives precise control over how a data sequence is traversed. This is the mechanism Python uses internally for all `for` loops.

```python
# iterators/log_iterator.py


class LogFileIterator:
    """
    Streams lines from a log file one at a time.
    Only one line is in memory at any point.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self._file = None

    def __iter__(self):
        """
        Called when iteration begins (e.g., start of a for loop).
        Opens the file and returns self as the iterator.
        """
        self._file = open(self.filepath, "r")
        return self

    def __next__(self):
        """
        Called on each iteration step.
        Returns the next line, or raises StopIteration at EOF.
        """
        line = self._file.readline()
        if not line:
            self._file.close()
            raise StopIteration
        return line.strip()


# --- Demonstration ---

LOGFILE = "/tmp/demo_pipeline.log"

error_count = 0
iterator = LogFileIterator(LOGFILE)

for line in iterator:
    if "ERROR" in line:
        error_count += 1

print(f"Total ERROR lines: {error_count}")
print("Memory used: approximately one line at a time.")
```

### What Python Does Internally

When you write `for line in iterator:`, Python calls:
1. `iterator.__iter__()` — initializes the iteration
2. `iterator.__next__()` — repeatedly until `StopIteration` is raised

This two-method protocol is the **iterator protocol** — it is what makes any object work in a `for` loop.

---

## 16.3 — Generator Functions

### Converting the Iterator to a Generator

A generator function uses `yield` instead of `return`. Python automatically implements `__iter__` and `__next__` — you write only the traversal logic.

```python
# generators/log_generator.py


def read_log_lines(filepath: str):
    """
    Generator function — streams lines from a log file.
    Each call to next() resumes execution from the last yield.
    """
    with open(filepath, "r") as f:
        for line in f:
            yield line.strip()


# --- Demonstration ---

LOGFILE = "/tmp/demo_pipeline.log"

# The generator object is created immediately — no file reading yet
gen = read_log_lines(LOGFILE)
print(type(gen))   # <class 'generator'>

# Reading begins only when we iterate
first  = next(gen)
second = next(gen)
third  = next(gen)

print(f"Line 1: {first}")
print(f"Line 2: {second}")
print(f"Line 3: {third}")
```

**Output:**
```
<class 'generator'>
Line 1: 2024-01-15 10:00:00 | INFO | Record processed: id=0
Line 2: 2024-01-15 10:01:00 | INFO | Record processed: id=1
Line 3: 2024-01-15 10:02:00 | INFO | Record processed: id=2
```

### Demonstrating Exhaustion

A generator can only be traversed once. Once exhausted, it yields nothing further.

```python
# demo_exhaustion.py

def count_up_to(n: int):
    for i in range(n):
        yield i


gen = count_up_to(3)

# First pass — works
print("First pass:", list(gen))   # [0, 1, 2]

# Second pass — exhausted
print("Second pass:", list(gen))  # []
```

**Output:**
```
First pass: [0, 1, 2]
Second pass: []
```

This is intentional. Generators are single-use streams — like reading a pipe. If you need to re-iterate, re-call the generator function.

---

## 16.4 — Generator Pipeline Chaining

### The Core Pattern for Streaming ETL

Each stage in the pipeline is a generator that reads from another generator. No stage produces a full list — each record flows through the chain one at a time.

```python
# pipeline/streaming_pipeline.py

from typing import Iterator, Dict, Any
import re


# --- Stage 1: Read raw lines from file ---

def read_file(filepath: str) -> Iterator[str]:
    """Yields one raw line at a time from the source file."""
    with open(filepath, "r") as f:
        for line in f:
            yield line.strip()


# --- Stage 2: Filter — keep only ERROR lines ---

def filter_errors(lines: Iterator[str]) -> Iterator[str]:
    """
    Receives a line iterator, yields only lines containing ERROR.
    Does not buffer — passes records through immediately.
    """
    for line in lines:
        if "ERROR" in line:
            yield line


# --- Stage 3: Transform — parse line into structured record ---

LOG_PATTERN = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
    r" \| (?P<level>\w+)"
    r" \| (?P<message>.+)"
)

def transform_records(lines: Iterator[str]) -> Iterator[Dict[str, Any]]:
    """
    Parses raw log lines into structured dictionaries.
    Skips lines that do not match the expected format.
    """
    for line in lines:
        match = LOG_PATTERN.match(line)
        if match:
            yield match.groupdict()
        else:
            print(f"[WARN] Skipping malformed line: {line[:60]}")


# --- Stage 4: Terminal — consume the pipeline and count ---

def count_records(records: Iterator[Dict[str, Any]]) -> int:
    """
    Consumes the generator chain.
    Returns total record count.
    Execution of all upstream stages happens here.
    """
    count = 0
    for record in records:
        count += 1
    return count


# --- Compose the pipeline ---

if __name__ == "__main__":
    LOGFILE = "/tmp/demo_pipeline.log"

    # Build the chain — no execution yet
    lines    = read_file(LOGFILE)
    errors   = filter_errors(lines)
    records  = transform_records(errors)

    # Execution begins here — records flow one at a time through all stages
    total = count_records(records)
    print(f"Total ERROR records processed: {total}")
```

### Demonstrating That Execution is Lazy

```python
# demo_lazy_execution.py


def read_file_verbose(filepath: str):
    print("[read_file] Generator created — no reading yet.")
    with open(filepath, "r") as f:
        for line in f:
            print(f"[read_file] Yielding line.")
            yield line.strip()


def filter_errors_verbose(lines):
    print("[filter_errors] Generator created — no filtering yet.")
    for line in lines:
        if "ERROR" in line:
            print(f"[filter_errors] Yielding error line.")
            yield line


LOGFILE = "/tmp/demo_pipeline.log"

# Nothing printed yet — generators are objects, not running code
lines  = read_file_verbose(LOGFILE)
errors = filter_errors_verbose(lines)

print("\n--- Calling next() to pull first record ---\n")
record = next(errors)
print(f"\nReceived: {record[:60]}")
```

**Output (abbreviated):**
```
--- Calling next() to pull first record ---

[read_file] Generator created — no reading yet.
[filter_errors] Generator created — no filtering yet.
[read_file] Yielding line.
[read_file] Yielding line.
... (iterates until first ERROR found)
[filter_errors] Yielding error line.

Received: 2024-01-15 10:00:00 | ERROR | Record processed: id=0
```

Execution does not begin until the consumer (the terminal stage) pulls a record. Each `next()` call propagates back through the chain — **pulling** data rather than pushing it.

---

## 16.5 — Generator Expressions

### Comparison with List Comprehensions

```python
# demo_expressions.py

import sys

data = range(1_000_000)

# List comprehension — materializes all 1M integers in RAM
list_result = [x * 2 for x in data if x % 2 == 0]

# Generator expression — produces values on demand
gen_result = (x * 2 for x in data if x % 2 == 0)

print(f"List size in memory: {sys.getsizeof(list_result):,} bytes")
print(f"Generator size in memory: {sys.getsizeof(gen_result)} bytes")

# Both produce identical values when iterated
print(f"First value from list: {list_result[0]}")
print(f"First value from gen:  {next(gen_result)}")
```

**Output:**
```
List size in memory: 8,448,728 bytes
Generator size in memory: 104 bytes
First value from list: 0
First value from gen:  0
```

The generator expression is syntactically identical to a list comprehension — only the brackets differ. The memory difference at scale is dramatic.

### Using Generator Expressions in Pipelines

```python
# demo_gen_expression_pipeline.py

LOGFILE = "/tmp/demo_pipeline.log"

with open(LOGFILE, "r") as f:
    error_count = sum(
        1
        for line in f
        if "ERROR" in line
    )

print(f"ERROR line count: {error_count}")
```

This is a complete single-pass streaming pipeline in four lines. No intermediate list is created.

---

## 16.6 — One-Pass Limitation and Safe Reuse Pattern

### Demonstrating the One-Pass Problem

```python
# demo_one_pass_problem.py


def get_records():
    yield {"id": 1, "value": 100}
    yield {"id": 2, "value": 200}
    yield {"id": 3, "value": 300}


records = get_records()

# First consumer
total = sum(r["value"] for r in records)
print(f"Total: {total}")   # 600

# Second consumer — generator is exhausted
count = sum(1 for _ in records)
print(f"Count: {count}")   # 0 — UNEXPECTED
```

### Safe Reuse Pattern

If you need to iterate the same data more than once, re-call the generator function — do not reuse the generator object.

```python
# demo_safe_reuse.py

from typing import Callable, Iterator, Dict, Any


def get_records() -> Iterator[Dict[str, Any]]:
    """Always returns a fresh generator when called."""
    yield {"id": 1, "value": 100}
    yield {"id": 2, "value": 200}
    yield {"id": 3, "value": 300}


# Pattern: pass the factory function, not the generator instance
def process_pipeline(record_factory: Callable) -> None:
    total = sum(r["value"] for r in record_factory())
    count = sum(1 for _ in record_factory())
    print(f"Count: {count}, Total: {total}, Average: {total / count:.2f}")


process_pipeline(get_records)
```

**Output:**
```
Count: 3, Total: 600, Average: 200.00
```

Pass generator **functions** (factories), not generator **instances**, when multiple passes may be needed.

---

# Module 17: Config-Driven & Modular Design

## Scenario

Your pipeline runs in three environments: development, staging, and production. Each has different database hosts, file paths, and feature flags. Currently, engineers change these values directly in the code before each deployment. Last quarter, a production run was accidentally pointed at the development database. This module demonstrates how to eliminate hardcoded configuration from pipeline code.

---

## 17.1 — The Hardcoded Configuration Problem

### What This Looks Like

```python
# pipeline_hardcoded.py — BAD DESIGN (do not follow in production)

import psycopg2

# These values change per environment — they have no business being in source code
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "orders"
DB_USER     = "admin"
DB_PASSWORD = "supersecret123"

INPUT_PATH  = "/data/raw/orders.csv"
OUTPUT_PATH = "/data/processed/orders_out.csv"
BATCH_SIZE  = 1000


def run_pipeline():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    # ... rest of pipeline
    print("Pipeline complete.")


run_pipeline()
```

### Why This Fails in Production

- Credentials committed to version control — a security incident.
- Changing any value requires a code change and redeployment.
- No way to promote the same artifact from dev to prod without modifying source.
- Batch size, feature flags, and paths are buried inside code — invisible to ops teams.

---

## 17.2 — YAML-Based Configuration

### Config File Structure

```
project/
    config/
        config.yaml
    pipeline/
        reader.py
        writer.py
    main.py
```

### config/config.yaml

```yaml
# config/config.yaml

database:
  host: "localhost"
  port: 5432
  name: "orders_dev"
  user: "pipeline_user"
  password: "dev_password_only"

paths:
  input:  "/data/raw/orders.csv"
  output: "/data/processed/orders_out.csv"

pipeline:
  batch_size: 500
  enable_deduplication: true
  log_level: "INFO"
```

### Loading the Config

```python
# config/loader.py

import yaml
import os
from typing import Any, Dict


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Loads and returns the YAML configuration as a nested dictionary.
    Raises FileNotFoundError if the path does not exist.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    if config is None:
        raise ValueError(f"Config file is empty: {config_path}")

    return config
```

### Using Config Inside the Pipeline

```python
# pipeline/reader.py

from typing import List, Dict, Any


class OrderReader:
    """
    Reads order records from a CSV file.
    All configuration is injected — no hardcoded values.
    """

    def __init__(self, config: Dict[str, Any]):
        self.input_path  = config["paths"]["input"]
        self.batch_size  = config["pipeline"]["batch_size"]

    def read(self) -> List[Dict[str, Any]]:
        print(f"[OrderReader] Reading from: {self.input_path}")
        print(f"[OrderReader] Batch size:   {self.batch_size}")
        # Simulated records — replace with actual CSV reading
        return [{"order_id": str(i), "amount": i * 10.0} for i in range(1, 6)]
```

```python
# main.py

from config.loader import load_config
from pipeline.reader import OrderReader


def main():
    config = load_config("config/config.yaml")

    print(f"[Main] Database host: {config['database']['host']}")
    print(f"[Main] Log level:     {config['pipeline']['log_level']}")

    reader = OrderReader(config)
    records = reader.read()

    for record in records:
        print(f"  {record}")


if __name__ == "__main__":
    main()
```

**Output:**
```
[Main] Database host: localhost
[Main] Log level:     INFO
[OrderReader] Reading from: /data/raw/orders.csv
[OrderReader] Batch size:   500
  {'order_id': '1', 'amount': 10.0}
  {'order_id': '2', 'amount': 20.0}
  ...
```

---

## 17.3 — Environment-Based Configuration

### Folder Structure with Per-Environment Configs

```
project/
    config/
        dev.yaml
        prod.yaml
    pipeline/
        reader.py
    config/
        loader.py
    main.py
```

### config/dev.yaml

```yaml
# config/dev.yaml

database:
  host: "localhost"
  port: 5432
  name: "orders_dev"
  user: "dev_user"
  password: "dev_pass"

paths:
  input:  "/data/dev/raw/orders.csv"
  output: "/data/dev/processed/orders_out.csv"

pipeline:
  batch_size: 100
  enable_deduplication: false
  log_level: "DEBUG"
```

### config/prod.yaml

```yaml
# config/prod.yaml

database:
  host: "prod-db.internal.company.com"
  port: 5432
  name: "orders_production"
  user: "pipeline_svc"
  password: "${DB_PASSWORD}"    # Resolved from environment variable at runtime

paths:
  input:  "/mnt/data/raw/orders.csv"
  output: "/mnt/data/processed/orders_out.csv"

pipeline:
  batch_size: 5000
  enable_deduplication: true
  log_level: "WARNING"
```

### Environment-Aware Config Loader

```python
# config/loader.py  (updated)

import yaml
import os
from typing import Any, Dict

CONFIG_DIR = os.path.join(os.path.dirname(__file__))


def load_config() -> Dict[str, Any]:
    """
    Loads config for the current environment.

    Reads the ENV environment variable to determine which config file to use.
    Defaults to 'dev' if ENV is not set.

    Usage:
        export ENV=prod   # on the production server
        export ENV=dev    # on a developer workstation
    """
    env = os.environ.get("ENV", "dev").lower()
    config_file = os.path.join(CONFIG_DIR, f"{env}.yaml")

    if not os.path.exists(config_file):
        raise FileNotFoundError(
            f"No config file found for environment '{env}': {config_file}"
        )

    print(f"[ConfigLoader] Loading config for environment: {env}")
    with open(config_file, "r") as f:
        return yaml.safe_load(f)
```

### Running in Different Environments

```bash
# Development (default)
python main.py

# Staging
ENV=staging python main.py

# Production
ENV=prod python main.py

# Or set it persistently in the shell session
export ENV=prod
python main.py
```

### main.py using Environment-Aware Loader

```python
# main.py

import os
# Simulate environment for this demo — in production, set via shell
os.environ.setdefault("ENV", "dev")

from config.loader import load_config
from pipeline.reader import OrderReader


def main():
    config = load_config()

    print(f"  DB Host:    {config['database']['host']}")
    print(f"  Log Level:  {config['pipeline']['log_level']}")
    print(f"  Batch Size: {config['pipeline']['batch_size']}")

    reader = OrderReader(config)
    records = reader.read()
    print(f"\n[Main] Loaded {len(records)} records.")


if __name__ == "__main__":
    main()
```

---

## 17.4 — Feature Flags in Config

### What Feature Flags Are

Feature flags are boolean configuration values that toggle pipeline behavior on or off without code changes. They enable gradual rollouts, A/B testing of pipeline logic, and safe disabling of experimental steps.

### config/dev.yaml (with feature flags)

```yaml
# config/dev.yaml  (excerpt with feature flags added)

pipeline:
  batch_size: 100
  log_level: "DEBUG"

features:
  enable_deduplication: true
  enable_schema_validation: false
  enable_audit_trail: false
  use_legacy_date_parser: true
```

### Using Feature Flags in the Pipeline

```python
# pipeline/transformer.py

from typing import List, Dict, Any


class RecordTransformer:
    """
    Applies transformations to pipeline records.
    Feature flags control which transformations are active.
    """

    def __init__(self, config: Dict[str, Any]):
        self.features = config.get("features", {})

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if self.features.get("enable_deduplication", False):
            records = self._deduplicate(records)
            print(f"[Transformer] Deduplication applied.")

        if self.features.get("enable_schema_validation", False):
            records = self._validate_schema(records)
            print(f"[Transformer] Schema validation applied.")

        if self.features.get("enable_audit_trail", False):
            self._write_audit_trail(records)
            print(f"[Transformer] Audit trail written.")

        return records

    def _deduplicate(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        unique = []
        for record in records:
            key = record.get("order_id")
            if key not in seen:
                seen.add(key)
                unique.append(record)
        return unique

    def _validate_schema(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        required_fields = {"order_id", "amount"}
        return [r for r in records if required_fields.issubset(r.keys())]

    def _write_audit_trail(self, records: List[Dict[str, Any]]) -> None:
        print(f"[AuditTrail] Writing {len(records)} records to audit log.")


# --- Demonstration ---

if __name__ == "__main__":
    import os
    os.environ.setdefault("ENV", "dev")

    import sys
    sys.path.insert(0, ".")
    from config.loader import load_config

    config = load_config()
    transformer = RecordTransformer(config)

    raw_records = [
        {"order_id": "1001", "amount": 250.0},
        {"order_id": "1002", "amount": 175.5},
        {"order_id": "1001", "amount": 250.0},   # Duplicate
        {"order_id": "1003"},                     # Missing amount
    ]

    result = transformer.transform(raw_records)
    print(f"\nRecords after transformation: {len(result)}")
    for r in result:
        print(f"  {r}")
```

**Output (with `enable_deduplication: true`, others false):**
```
[Transformer] Deduplication applied.

Records after transformation: 3
  {'order_id': '1001', 'amount': 250.0}
  {'order_id': '1002', 'amount': 175.5}
  {'order_id': '1003'}
```

---

## 17.5 — Config Validation

### Why Validation is Required

Pipelines fail at runtime — often hours in — when a config key is missing or has the wrong type. Config validation catches these errors at startup, before any data processing begins.

```python
# config/validator.py

from typing import Any, Dict, List


class ConfigValidationError(Exception):
    """Raised when the loaded configuration is structurally invalid."""
    pass


REQUIRED_KEYS = [
    ("database", "host"),
    ("database", "port"),
    ("database", "name"),
    ("paths", "input"),
    ("paths", "output"),
    ("pipeline", "batch_size"),
    ("pipeline", "log_level"),
]


def validate_config(config: Dict[str, Any]) -> None:
    """
    Validates that all required configuration keys are present.
    Raises ConfigValidationError with a detailed message if any are missing.
    """
    missing: List[str] = []

    for section, key in REQUIRED_KEYS:
        if section not in config:
            missing.append(f"[{section}]")
        elif key not in config[section]:
            missing.append(f"[{section}].{key}")

    if missing:
        raise ConfigValidationError(
            f"Configuration is missing required keys:\n" +
            "\n".join(f"  - {m}" for m in missing)
        )

    # Type checks
    batch_size = config["pipeline"]["batch_size"]
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ConfigValidationError(
            f"[pipeline].batch_size must be a positive integer. Got: {batch_size!r}"
        )

    print("[ConfigValidator] Configuration is valid.")
```

### Updated Loader with Validation

```python
# config/loader.py  (final version with validation)

import yaml
import os
from typing import Any, Dict
from config.validator import validate_config

CONFIG_DIR = os.path.dirname(__file__)


def load_config() -> Dict[str, Any]:
    """
    Loads, validates, and returns the environment-specific configuration.
    Raises on missing file, empty config, or failed validation.
    """
    env = os.environ.get("ENV", "dev").lower()
    config_file = os.path.join(CONFIG_DIR, f"{env}.yaml")

    if not os.path.exists(config_file):
        raise FileNotFoundError(
            f"No config file for environment '{env}': {config_file}"
        )

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    if not config:
        raise ValueError(f"Config file is empty: {config_file}")

    validate_config(config)   # Fail fast — before any pipeline logic runs
    return config
```

### Demonstrating Graceful Error Handling

```python
# demo_config_validation.py

from config.validator import validate_config, ConfigValidationError


# Simulate an incomplete config (e.g., missing database section entirely)
incomplete_config = {
    "paths": {
        "input": "/data/raw/orders.csv",
        "output": "/data/processed/orders_out.csv"
    },
    "pipeline": {
        "batch_size": -1,    # Invalid value
        "log_level": "DEBUG"
    }
    # database section missing
}

try:
    validate_config(incomplete_config)
except ConfigValidationError as e:
    print(f"[STARTUP ERROR] Pipeline cannot start:\n{e}")
```

**Output:**
```
[STARTUP ERROR] Pipeline cannot start:
Configuration is missing required keys:
  - [database]
```

The pipeline exits cleanly at startup with a clear diagnostic. No data is processed. No ambiguous runtime error occurs 3 hours into a batch job.

---

## 17.6 — Complete Modular Project Structure

### Final Structure

```
project/
    config/
        __init__.py
        dev.yaml
        prod.yaml
        loader.py
        validator.py
    pipeline/
        __init__.py
        reader.py
        transformer.py
        writer.py
    main.py
```

### pipeline/writer.py

```python
# pipeline/writer.py

from typing import List, Dict, Any


class OrderWriter:
    """
    Writes processed records to the output path defined in config.
    """

    def __init__(self, config: Dict[str, Any]):
        self.output_path = config["paths"]["output"]

    def write(self, records: List[Dict[str, Any]]) -> None:
        print(f"[OrderWriter] Writing {len(records)} records to: {self.output_path}")
        # In production: write to CSV, Parquet, database, etc.
        for record in records:
            print(f"  Written: {record}")
```

### main.py — Full Pipeline Entry Point

```python
# main.py

import os
import sys

# Set environment — in production this comes from the shell or container env var
os.environ.setdefault("ENV", "dev")

from config.loader import load_config
from config.validator import ConfigValidationError
from pipeline.reader import OrderReader
from pipeline.transformer import RecordTransformer
from pipeline.writer import OrderWriter


def run_pipeline() -> None:
    # Step 1 — Load and validate config (fail fast)
    try:
        config = load_config()
    except (FileNotFoundError, ValueError, ConfigValidationError) as e:
        print(f"[FATAL] Pipeline startup failed: {e}", file=sys.stderr)
        sys.exit(1)

    # Step 2 — Initialize pipeline components with config
    reader      = OrderReader(config)
    transformer = RecordTransformer(config)
    writer      = OrderWriter(config)

    # Step 3 — Execute pipeline stages
    print("\n[Pipeline] Starting ingestion...")
    records = reader.read()

    print("\n[Pipeline] Starting transformation...")
    transformed = transformer.transform(records)

    print("\n[Pipeline] Starting write...")
    writer.write(transformed)

    print("\n[Pipeline] Completed successfully.")


if __name__ == "__main__":
    run_pipeline()
```

**Output (ENV=dev):**
```
[ConfigLoader] Loading config for environment: dev
[ConfigValidator] Configuration is valid.

[Pipeline] Starting ingestion...
[OrderReader] Reading from: /data/dev/raw/orders.csv
[OrderReader] Batch size:   100

[Pipeline] Starting transformation...
[Transformer] Deduplication applied.

[Pipeline] Starting write...
[OrderWriter] Writing 3 records to: /data/dev/processed/orders_out.csv
  Written: {'order_id': '1', 'amount': 10.0}
  Written: {'order_id': '2', 'amount': 20.0}
  Written: {'order_id': '3', 'amount': 30.0}

[Pipeline] Completed successfully.
```

### Switching to Production — No Code Changes Required

```bash
export ENV=prod
python main.py
```

The same binary artifact runs against production configuration. The code is never touched between environments.

---

