"""
pipeline/readers.py
Concrete DataSource implementations.
Module 15 – Inheritance, super(), method overriding.
Module 16 – Generators / lazy reading.
"""

from __future__ import annotations

import csv
import gzip
import logging
import random
import time
from pathlib import Path
from typing import Iterator

from pipeline.base import DataSource, Record, RecordStream
from pipeline.decorators import timer
from pipeline.exceptions import DataReadError, SchemaError

logger = logging.getLogger(__name__)

# Minimum columns every CSV source must provide
REQUIRED_COLUMNS = {
    "order_id", "customer_id", "product_id", "product_name",
    "category", "quantity", "unit_price", "region", "order_date", "status",
}


# ── CSVReader ──────────────────────────────────────────────────────────────────

class CSVReader(DataSource):
    """
    Reads a (possibly gzip-compressed) CSV file lazily row-by-row.

    Module 16: read() is a generator – the file is never fully loaded
    into memory, making it safe for files with millions of rows.

    Config keys:
        path        – path to the CSV file (required).
        encoding    – file encoding (default utf-8).
        chunk_size  – not used here; retained for API parity with DBReader.
        skip_header – skip the first non-comment line (default False).
    """

    def __init__(self, config: dict):
        super().__init__(config)              # Module 15 – super()
        self._path = Path(config["path"])
        self._encoding = config.get("encoding", "utf-8")
        self._fh = None                       # file handle opened in connect()
        self._fieldnames: list[str] = []

    # ── DataSource interface ───────────────────────────────────────────────────

    def connect(self) -> None:
        if not self._path.exists():
            raise DataReadError(
                f"CSV file not found: {self._path}",
                context={"path": str(self._path)},
            )
        opener = gzip.open if self._path.suffix == ".gz" else open
        self._fh = opener(self._path, "rt", encoding=self._encoding, newline="")
        self._connected = True
        self.logger.info("📂  Connected to CSV: %s", self._path)

    @timer(label="CSVReader.read")
    def read(self) -> RecordStream:
        """
        Generator: yields one record dict per CSV row.
        Memory usage stays flat regardless of file size – Module 16.
        """
        if not self._connected or self._fh is None:
            raise DataReadError("CSVReader.read() called before connect()")

        reader = csv.DictReader(self._fh)

        # ── Schema validation (Module 20) ──────────────────────────────────────
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise SchemaError(
                f"CSV is missing required columns: {missing}",
                context={"file": str(self._path)},
            )

        row_num = 0
        for row_num, row in enumerate(reader, start=1):
            # Strip whitespace from all string values
            yield {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

        self.logger.info("📖  CSVReader yielded %d rows", row_num)

    def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None
            self._connected = False
            self.logger.debug("🔒  CSVReader closed: %s", self._path)


# ── MockDBReader ───────────────────────────────────────────────────────────────

class MockDBReader(DataSource):
    """
    Simulates reading from a database in chunks (like SQLAlchemy cursor iteration).

    Demonstrates:
        - Method overriding: read() yields paginated chunks.
        - super() used in __init__.
        - Realistic latency simulation.

    Config keys:
        host       – DB host (cosmetic in this mock).
        database   – DB name (cosmetic).
        table      – table name.
        batch_size – rows per "fetch" (default 1000).
        total_rows – how many rows to simulate (default 50000).
    """

    def __init__(self, config: dict):
        super().__init__(config)              # Module 15 – super()
        self._batch_size = config.get("batch_size", 1_000)
        self._total_rows = config.get("total_rows", 50_000)
        self._table      = config.get("table", "sales")
        self._cursor: Iterator | None = None

    def connect(self) -> None:
        self._connected = True
        self.logger.info(
            "🗄  MockDBReader connected → %s.%s (%d rows)",
            self.config.get("database", "mydb"), self._table, self._total_rows,
        )

    @timer(label="MockDBReader.read")
    def read(self) -> RecordStream:
        """
        Generator that yields rows in batches, simulating cursor pagination.
        Module 16: chunked lazy reading pattern.
        """
        if not self._connected:
            raise DataReadError("MockDBReader.read() called before connect()")

        categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports"]
        regions    = ["North", "South", "East", "West", "Central"]
        statuses   = ["Delivered", "Shipped", "Returned", "Cancelled"]
        random.seed(99)

        fetched = 0
        batch_no = 0
        while fetched < self._total_rows:
            batch_no += 1
            this_batch = min(self._batch_size, self._total_rows - fetched)
            # Simulate DB round-trip latency
            time.sleep(0.001)
            self.logger.debug("  DB batch %d: fetching %d rows …", batch_no, this_batch)

            for _ in range(this_batch):
                fetched += 1
                yield {
                    "order_id":         f"DB-{fetched:07d}",
                    "customer_id":      f"CUST-{random.randint(1, 20000):05d}",
                    "product_id":       f"PRD-{random.randint(1000, 9999):04d}",
                    "product_name":     f"Product-{random.randint(1, 200)}",
                    "category":         random.choice(categories),
                    "sub_category":     "General",
                    "quantity":         random.randint(1, 10),
                    "unit_price":       round(random.uniform(5.0, 999.0), 2),
                    "discount_pct":     round(random.choice([0, 0.05, 0.10, 0.15, 0.20]), 2),
                    "region":           random.choice(regions),
                    "state":            "N/A",
                    "order_date":       f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                    "ship_date":        f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                    "ship_mode":        "Standard Class",
                    "customer_segment": "Consumer",
                    "status":           random.choices(statuses, [0.78, 0.12, 0.06, 0.04])[0],
                }

        self.logger.info("🗄  MockDBReader yielded %d rows", fetched)

    def close(self) -> None:
        self._connected = False
        self.logger.debug("🔒  MockDBReader disconnected")


# ── ChunkedCSVReader – extends CSVReader to yield list[Record] batches ─────────

class ChunkedCSVReader(CSVReader):
    """
    Extends CSVReader (method overriding) to yield fixed-size batches of records
    instead of individual rows – useful for parallel chunk processing.
    Module 15: overrides read(); calls super().connect() / super().close().
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self._chunk_size = config.get("chunk_size", 10_000)

    def read(self) -> RecordStream:  # type: ignore[override]
        """Override: yields list[Record] chunks instead of individual Record."""
        chunk: list[Record] = []
        for record in super().read():          # delegates to CSVReader.read()
            chunk.append(record)
            if len(chunk) >= self._chunk_size:
                yield chunk                    # type: ignore[misc]
                chunk = []
        if chunk:
            yield chunk                        # type: ignore[misc]
