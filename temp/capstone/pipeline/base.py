"""
pipeline/base.py
Abstract Base Classes for every stage of the pipeline.
Module 15 – Abstract Base Classes (abc), super(), method overriding.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Generator, Any

logger = logging.getLogger(__name__)

# Type alias used throughout the codebase
Record = dict[str, Any]
RecordStream = Generator[Record, None, None]


# ── DataSource ─────────────────────────────────────────────────────────────────

class DataSource(ABC):
    """
    Abstract base for all data readers.

    Subclasses must implement:
        connect()   – open the underlying resource.
        read()      – yield Record dicts lazily (generator).
        close()     – release the resource.
    """

    def __init__(self, config: dict):
        self.config = config
        self._connected = False
        self.logger = logging.getLogger(self.__class__.__name__)

    # ── lifecycle ──────────────────────────────────────────────────────────────

    @abstractmethod
    def connect(self) -> None:
        """Open / authenticate the underlying data source."""

    @abstractmethod
    def read(self) -> RecordStream:
        """Yield records lazily; one dict per row/document."""

    @abstractmethod
    def close(self) -> None:
        """Release any held resources (file handles, DB connections, …)."""

    # ── context-manager support ────────────────────────────────────────────────

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    # ── helper ────────────────────────────────────────────────────────────────

    @property
    def source_name(self) -> str:
        return self.config.get("name", self.__class__.__name__)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(source={self.source_name!r})"


# ── DataTransformer ────────────────────────────────────────────────────────────

class DataTransformer(ABC):
    """
    Abstract base for transformation stages.

    Subclasses must implement:
        transform(records) – consume a RecordStream, yield transformed Records.
    """

    def __init__(self, config: dict | None = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self._stats: dict[str, int] = {"in": 0, "out": 0, "dropped": 0, "errors": 0}

    @abstractmethod
    def transform(self, records: RecordStream) -> RecordStream:
        """Consume an input stream, yield an output stream."""

    # ── pipe operator so stages can be composed: a | b | c ────────────────────

    def __or__(self, other: "DataTransformer") -> "_ComposedTransformer":
        return _ComposedTransformer(self, other)

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(stats={self._stats})"


class _ComposedTransformer(DataTransformer):
    """Internal: chains two transformers using the | operator."""

    def __init__(self, left: DataTransformer, right: DataTransformer):
        super().__init__()
        self._left = left
        self._right = right

    def transform(self, records: RecordStream) -> RecordStream:
        return self._right.transform(self._left.transform(records))


# ── DataWriter ─────────────────────────────────────────────────────────────────

class DataWriter(ABC):
    """
    Abstract base for all data writers / sinks.

    Subclasses must implement:
        open()    – prepare the sink (create file, open DB cursor, …).
        write()   – consume a RecordStream and persist records.
        close()   – flush and release resources.
    """

    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._rows_written = 0

    @abstractmethod
    def open(self) -> None:
        """Prepare the sink."""

    @abstractmethod
    def write(self, records: RecordStream) -> int:
        """
        Consume records and persist them.
        Returns the number of rows written.
        """

    @abstractmethod
    def close(self) -> None:
        """Flush and close the sink."""

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def rows_written(self) -> int:
        return self._rows_written


# ── Pipeline orchestrator ──────────────────────────────────────────────────────

class Pipeline:
    """
    Assembles source → [transformers …] → writer and orchestrates execution.
    Uses super() in subclasses to extend behaviour.
    """

    def __init__(
        self,
        source: DataSource,
        transformers: list[DataTransformer],
        writer: DataWriter,
        name: str = "Pipeline",
    ):
        self.source       = source
        self.transformers  = transformers
        self.writer        = writer
        self.name          = name
        self.logger        = logging.getLogger(self.__class__.__name__)

    def run(self) -> dict:
        """Execute the full pipeline; return a summary dict."""
        self.logger.info("🚀  Pipeline [%s] starting …", self.name)
        start = __import__("time").perf_counter()

        with self.source as src, self.writer as wrt:
            stream: RecordStream = src.read()
            for transformer in self.transformers:
                stream = transformer.transform(stream)
            rows = wrt.write(stream)

        elapsed = __import__("time").perf_counter() - start
        summary = {
            "pipeline": self.name,
            "rows_written": rows,
            "elapsed_s": round(elapsed, 3),
        }
        self.logger.info("🏁  Pipeline [%s] done. %s", self.name, summary)
        return summary

    def __repr__(self) -> str:
        return (
            f"Pipeline(name={self.name!r}, source={self.source!r}, "
            f"transformers={len(self.transformers)}, writer={self.writer!r})"
        )
