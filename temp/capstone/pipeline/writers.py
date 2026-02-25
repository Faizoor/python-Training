"""
pipeline/writers.py
Concrete DataWriter implementations.
Module 15 – Inheritance, super(), method overriding.
Module 20 – DataWriteError raised on I/O failure.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from pathlib import Path

from pipeline.base import DataWriter, RecordStream
from pipeline.decorators import timer
from pipeline.exceptions import DataWriteError

logger = logging.getLogger(__name__)


# ── CSVWriter ──────────────────────────────────────────────────────────────────

class CSVWriter(DataWriter):
    """
    Writes a RecordStream to a CSV file row-by-row (streaming write;
    never holds the full dataset in memory).

    Config keys:
        path       – output file path (required).
        fieldnames – column order (auto-detected from first record if omitted).
        encoding   – default utf-8.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self._path      = Path(config["path"])
        self._encoding  = config.get("encoding", "utf-8")
        self._fieldnames= config.get("fieldnames")   # optional
        self._fh        = None
        self._writer    = None

    def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._fh = open(self._path, "w", newline="", encoding=self._encoding)
        except OSError as exc:
            raise DataWriteError(f"Cannot open {self._path}: {exc}") from exc
        self.logger.info("📝  CSVWriter opened: %s", self._path)

    @timer(label="CSVWriter.write")
    def write(self, records: RecordStream) -> int:
        if self._fh is None:
            raise DataWriteError("CSVWriter.write() called before open()")

        first = True
        for rec in records:
            if first:
                fields = self._fieldnames or list(rec.keys())
                self._writer = csv.DictWriter(
                    self._fh, fieldnames=fields, extrasaction="ignore"
                )
                self._writer.writeheader()
                first = False
            self._writer.writerow(rec)
            self._rows_written += 1

            if self._rows_written % 25_000 == 0:
                self.logger.debug("  … CSVWriter wrote %d rows", self._rows_written)

        self.logger.info("✅  CSVWriter wrote %d rows → %s", self._rows_written, self._path)
        return self._rows_written

    def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None


# ── JSONWriter ─────────────────────────────────────────────────────────────────

class JSONWriter(DataWriter):
    """
    Writes a RecordStream as a JSON array to disk.
    Materialises the entire stream in memory – suitable for aggregated outputs.

    Config keys:
        path   – output file path (required).
        indent – JSON indent level (default 2).
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self._path   = Path(config["path"])
        self._indent = config.get("indent", 2)
        self._buffer: list = []

    def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._buffer = []
        self.logger.info("📝  JSONWriter ready: %s", self._path)

    @timer(label="JSONWriter.write")
    def write(self, records: RecordStream) -> int:
        self._buffer = list(records)
        self._rows_written = len(self._buffer)
        try:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self._buffer, f, indent=self._indent, default=str)
        except OSError as exc:
            raise DataWriteError(f"Cannot write JSON to {self._path}: {exc}") from exc

        self.logger.info("✅  JSONWriter wrote %d records → %s",
                         self._rows_written, self._path)
        return self._rows_written

    def close(self) -> None:
        self._buffer = []


# ── SummaryWriter ──────────────────────────────────────────────────────────────

class SummaryWriter(DataWriter):
    """
    Produces a human-readable Markdown summary report from aggregated records.
    Extends DataWriter; overrides write() completely.

    Demonstrates method overriding (Module 15): write() has different
    logic than CSVWriter.write() even though signature is identical.
    """

    _TOP_N = 10  # rows per leaderboard table

    def __init__(self, config: dict):
        super().__init__(config)
        self._path = Path(config["path"])
        self._run_info: dict = config.get("run_info", {})

    def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info("📝  SummaryWriter ready: %s", self._path)

    @timer(label="SummaryWriter.write")
    def write(self, records: RecordStream) -> int:
        rows = list(records)
        self._rows_written = len(rows)
        if not rows:
            logger.warning("SummaryWriter received no records")
            return 0

        lines = self._build_markdown(rows)
        with open(self._path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        self.logger.info("✅  SummaryWriter wrote report → %s", self._path)
        return self._rows_written

    def close(self) -> None:
        pass

    # ── markdown builder ───────────────────────────────────────────────────────

    def _build_markdown(self, rows: list[dict]) -> list[str]:
        lines = [
            "# E-Commerce Sales Analytics – Pipeline Report",
            "",
            f"**Generated:** {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ]
        if self.run_info:
            for k, v in self.run_info.items():
                lines.append(f"**{k}:** {v}")

        lines += ["", "---", ""]

        # ── Totals ─────────────────────────────────────────────────────────────
        total_orders   = sum(r["order_count"]   for r in rows)
        total_revenue  = sum(r["net_revenue"]   for r in rows)
        total_margin   = sum(r["gross_margin"]  for r in rows)
        total_discount = sum(r["discount_amount"] for r in rows)

        lines += [
            "## Overall KPIs",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Total Orders     | {total_orders:,} |",
            f"| Net Revenue      | ${total_revenue:,.2f} |",
            f"| Gross Margin     | ${total_margin:,.2f} |",
            f"| Margin %         | {total_margin/total_revenue*100:.1f} % |" if total_revenue else "| Margin % | N/A |",
            f"| Discounts Given  | ${total_discount:,.2f} |",
            "",
        ]

        # ── Revenue by Region ─────────────────────────────────────────────────
        region_totals: dict[str, float] = {}
        for r in rows:
            region_totals[r["region"]] = region_totals.get(r["region"], 0) + r["net_revenue"]

        lines += ["## Net Revenue by Region", "", "| Region | Net Revenue |", "|--------|-------------|"]
        for region, rev in sorted(region_totals.items(), key=lambda x: -x[1]):
            lines.append(f"| {region} | ${rev:,.2f} |")

        # ── Revenue by Category ───────────────────────────────────────────────
        cat_totals: dict[str, float] = {}
        for r in rows:
            cat_totals[r["category"]] = cat_totals.get(r["category"], 0) + r["net_revenue"]

        lines += ["", "## Net Revenue by Category", "", "| Category | Net Revenue |", "|----------|-------------|"]
        for cat, rev in sorted(cat_totals.items(), key=lambda x: -x[1]):
            lines.append(f"| {cat} | ${rev:,.2f} |")

        # ── Top buckets by margin ─────────────────────────────────────────────
        top = sorted(rows, key=lambda r: r["gross_margin"], reverse=True)[: self._TOP_N]
        lines += [
            "", f"## Top {self._TOP_N} Region-Category-Quarter Buckets by Gross Margin", "",
            "| Region | Category | Year | Quarter | Orders | Net Revenue | Margin |",
            "|--------|----------|------|---------|--------|-------------|--------|",
        ]
        for r in top:
            lines.append(
                f"| {r['region']} | {r['category']} | {r['year']} | {r['quarter']} "
                f"| {r['order_count']:,} | ${r['net_revenue']:,.2f} | ${r['gross_margin']:,.2f} |"
            )

        lines += ["", "---", "_Generated by the Advanced Python DE Capstone Pipeline._"]
        return lines

    @property
    def run_info(self) -> dict:
        return self._run_info
