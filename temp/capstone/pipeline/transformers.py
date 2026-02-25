"""
pipeline/transformers.py
Concrete DataTransformer implementations.
Module 15 – Inheritance, super(), method overriding.
Module 16 – Generators: each transform step is a lazy pipeline stage.
Module 18 – PipelineStep class decorator, @timer.
Module 20 – ValidationError raised and handled gracefully.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from pipeline.base import DataTransformer, Record, RecordStream
from pipeline.decorators import PipelineStep, timer
from pipeline.exceptions import ValidationError, TransformError

logger = logging.getLogger(__name__)


# ── helpers ────────────────────────────────────────────────────────────────────

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_date(value: Any) -> datetime | None:
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(value), fmt)
        except (ValueError, TypeError):
            pass
    return None


# ── 1. CleaningTransformer ─────────────────────────────────────────────────────

class CleaningTransformer(DataTransformer):
    """
    First stage: validates and coerces raw fields.
    Records that cannot be repaired are dropped (counter tracked in self._stats).

    Demonstrates:
        - super().__init__() in DataTransformer subclass.
        - ValidationError raised and caught; bad records skipped.
        - @PipelineStep class decorator on the generator.
    """

    # Default thresholds (can be overridden via config)
    _MAX_DISCOUNT = 1.0       # 100 %
    _MAX_QUANTITY = 500
    _MIN_PRICE    = 0.01

    def __init__(self, config: dict | None = None):
        super().__init__(config)              # Module 15 – super()
        self._seen_order_ids: set[str] = set()

    @PipelineStep(name="CleaningTransformer", log_every=20_000)
    def transform(self, records: RecordStream) -> RecordStream:
        for raw in records:
            self._stats["in"] += 1
            try:
                clean = self._clean(raw)
                self._stats["out"] += 1
                yield clean
            except ValidationError as exc:
                self._stats["dropped"] += 1
                self.logger.debug("⚠  Dropped record: %s", exc)
            except Exception as exc:
                self._stats["errors"] += 1
                self.logger.warning("⛔ Unexpected error on record %s: %s",
                                    raw.get("order_id", "?"), exc)

        self.logger.info(
            "CleaningTransformer stats: %s  (drop rate %.1f %%)",
            self._stats,
            100 * self._stats["dropped"] / max(self._stats["in"], 1),
        )

    def _clean(self, raw: Record) -> Record:
        """Validate and coerce a single record; raise ValidationError if invalid."""
        order_id = str(raw.get("order_id") or "").strip()
        if not order_id:
            raise ValidationError("Empty order_id", field="order_id")

        # Duplicate check
        if order_id in self._seen_order_ids:
            raise ValidationError("Duplicate order_id", field="order_id", value=order_id)
        self._seen_order_ids.add(order_id)

        # quantity
        quantity = _safe_int(raw.get("quantity"), default=-1)
        if quantity <= 0:
            raise ValidationError("Non-positive quantity", field="quantity", value=quantity)
        if quantity > self._MAX_QUANTITY:
            raise ValidationError("Quantity exceeds max", field="quantity", value=quantity)

        # unit_price
        unit_price = _safe_float(raw.get("unit_price"), default=0.0)
        if unit_price is None or unit_price < self._MIN_PRICE:
            raise ValidationError("Invalid unit_price", field="unit_price", value=unit_price)

        # discount
        discount_pct = _safe_float(raw.get("discount_pct"), default=0.0)
        if discount_pct > self._MAX_DISCOUNT:
            raise ValidationError("Discount > 100 %", field="discount_pct", value=discount_pct)
        discount_pct = max(0.0, discount_pct)

        # order_date
        order_dt = _safe_date(raw.get("order_date"))
        if order_dt is None:
            raise ValidationError("Unparseable order_date",
                                  field="order_date", value=raw.get("order_date"))

        # customer_id – nullable but we still require it
        customer_id = str(raw.get("customer_id") or "").strip()
        if not customer_id:
            raise ValidationError("Missing customer_id", field="customer_id")

        return {
            **raw,
            "order_id":     order_id,
            "customer_id":  customer_id,
            "quantity":     quantity,
            "unit_price":   unit_price,
            "discount_pct": discount_pct,
            "order_date":   order_dt.date().isoformat(),
            "order_year":   order_dt.year,
            "order_month":  order_dt.month,
            "order_quarter":f"Q{(order_dt.month - 1) // 3 + 1}",
        }


# ── 2. EnrichmentTransformer ───────────────────────────────────────────────────

class EnrichmentTransformer(DataTransformer):
    """
    Second stage: derives new columns from cleaned data.
    Injects async-enriched category metadata (passed in via config).
    """

    def __init__(self, config: dict | None = None, category_metadata: dict | None = None):
        super().__init__(config)
        # Async-fetched metadata injected here (see main.py)
        self._category_meta = category_metadata or {}

    @PipelineStep(name="EnrichmentTransformer", log_every=20_000)
    def transform(self, records: RecordStream) -> RecordStream:
        for rec in records:
            self._stats["in"] += 1
            try:
                yield self._enrich(rec)
                self._stats["out"] += 1
            except Exception as exc:
                self._stats["errors"] += 1
                self.logger.warning("Enrich error on %s: %s", rec.get("order_id"), exc)
                yield rec   # pass through un-enriched rather than drop

    def _enrich(self, rec: Record) -> Record:
        quantity   = rec["quantity"]
        unit_price = rec["unit_price"]
        discount   = rec["discount_pct"]

        # Financials
        gross_revenue  = round(quantity * unit_price, 2)
        discount_amount= round(gross_revenue * discount, 2)
        net_revenue    = round(gross_revenue - discount_amount, 2)
        # Approximate cost / margin (25-40 % cost depending on category)
        category = rec.get("category", "")
        cost_pct = self._category_meta.get(category, {}).get("avg_cost_pct", 0.30)
        cogs     = round(net_revenue * cost_pct, 2)
        margin   = round(net_revenue - cogs, 2)

        return {
            **rec,
            "gross_revenue":   gross_revenue,
            "discount_amount": discount_amount,
            "net_revenue":     net_revenue,
            "cogs":            cogs,
            "gross_margin":    margin,
            "margin_pct":      round(margin / net_revenue * 100, 2) if net_revenue else 0.0,
            "is_high_value":   net_revenue >= 500,
            "category_tier":   self._category_meta.get(category, {}).get("tier", "Standard"),
        }


# ── 3. AggregationTransformer ──────────────────────────────────────────────────

class AggregationTransformer(DataTransformer):
    """
    Aggregates a record stream into summary rows grouped by (region, category, year, quarter).
    Materialises state in memory – only suitable after cleaning/enrichment.
    Demonstrates: overriding transform(), maintaining internal state.
    """

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._agg: dict[tuple, dict] = {}

    @timer(label="AggregationTransformer.transform")
    def transform(self, records: RecordStream) -> RecordStream:
        self._agg.clear()

        for rec in records:
            self._stats["in"] += 1
            key = (
                rec.get("region", "Unknown"),
                rec.get("category", "Unknown"),
                _safe_int(rec.get("order_year", 0)),
                rec.get("order_quarter", "Q?"),
            )
            if key not in self._agg:
                self._agg[key] = {
                    "region":          key[0],
                    "category":        key[1],
                    "year":            key[2],
                    "quarter":         key[3],
                    "order_count":     0,
                    "total_quantity":  0,
                    "gross_revenue":   0.0,
                    "discount_amount": 0.0,
                    "net_revenue":     0.0,
                    "gross_margin":    0.0,
                    "returned_orders": 0,
                }
            bucket = self._agg[key]
            bucket["order_count"]    += 1
            bucket["total_quantity"] += _safe_int(rec.get("quantity", 0))
            bucket["gross_revenue"]  += _safe_float(rec.get("gross_revenue", 0.0))
            bucket["discount_amount"]+= _safe_float(rec.get("discount_amount", 0.0))
            bucket["net_revenue"]    += _safe_float(rec.get("net_revenue", 0.0))
            bucket["gross_margin"]   += _safe_float(rec.get("gross_margin", 0.0))
            if rec.get("status") == "Returned":
                bucket["returned_orders"] += 1

        for bucket in self._agg.values():
            # Round and compute derived aggregates
            bucket["gross_revenue"]   = round(bucket["gross_revenue"], 2)
            bucket["net_revenue"]     = round(bucket["net_revenue"], 2)
            bucket["gross_margin"]    = round(bucket["gross_margin"], 2)
            bucket["discount_amount"] = round(bucket["discount_amount"], 2)
            nr = bucket["net_revenue"]
            bucket["avg_margin_pct"]  = (
                round(bucket["gross_margin"] / nr * 100, 2) if nr else 0.0
            )
            bucket["return_rate_pct"] = (
                round(bucket["returned_orders"] / bucket["order_count"] * 100, 2)
                if bucket["order_count"]
                else 0.0
            )
            self._stats["out"] += 1
            yield bucket

        self.logger.info(
            "AggregationTransformer: %d records → %d summary buckets",
            self._stats["in"], self._stats["out"],
        )
