"""
tests/test_pipeline.py
──────────────────────────────────────────────────────────────────────────────
Pytest test suite for the capstone pipeline.
Module 20 – Unit testing entity classes and data validation logic.

Run:
    pytest tests/ -v
    pytest tests/ -v --tb=short       # compact tracebacks
    pytest tests/ -k "transform"      # filter by name
──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import csv
import json
from pathlib import Path
from typing import Generator
from unittest.mock import patch, MagicMock

import pytest

# ── add capstone root to sys.path ──────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.exceptions import (
    ValidationError,
    DataReadError,
    ConfigError,
    RetryExhaustedError,
    APIError,
    PipelineError,
)
from pipeline.transformers import CleaningTransformer, EnrichmentTransformer, AggregationTransformer
from pipeline.decorators import timer, retry, PipelineStep, log_step
from pipeline.base import DataSource, DataTransformer, DataWriter, Pipeline
from pipeline.readers import CSVReader, MockDBReader
from pipeline.writers import CSVWriter, JSONWriter
from utils.config_loader import load_config, validate_config, merge_configs


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

def _make_valid_record(**overrides) -> dict:
    """Factory: a minimal valid sales record."""
    base = {
        "order_id":         "ORD-0000001",
        "customer_id":      "CUST-00001",
        "product_id":       "ELE-COM-1234",
        "product_name":     "Laptop Pro 15",
        "category":         "Electronics",
        "sub_category":     "Computers",
        "quantity":         "2",
        "unit_price":       "899.99",
        "discount_pct":     "0.10",
        "region":           "East",
        "state":            "New York",
        "order_date":       "2024-03-15",
        "ship_date":        "2024-03-18",
        "ship_mode":        "First Class",
        "customer_segment": "Corporate",
        "status":           "Delivered",
    }
    return {**base, **overrides}


@pytest.fixture
def valid_record():
    return _make_valid_record()


@pytest.fixture
def valid_stream(valid_record):
    """Single-record stream."""
    return iter([valid_record])


@pytest.fixture
def multi_stream():
    """Stream of 5 distinct valid records."""
    return iter([_make_valid_record(order_id=f"ORD-{i:07d}") for i in range(1, 6)])


@pytest.fixture
def tmp_csv(tmp_path) -> Path:
    """Write a small CSV and return its path."""
    path = tmp_path / "sample.csv"
    rows = [_make_valid_record(order_id=f"ORD-{i:07d}") for i in range(1, 11)]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return path


# ═══════════════════════════════════════════════════════════════════════════════
# Module 20 – Custom Exceptions
# ═══════════════════════════════════════════════════════════════════════════════

class TestExceptions:
    def test_pipeline_error_base(self):
        exc = PipelineError("base error", context={"key": "val"})
        assert "base error" in str(exc)
        assert exc.context == {"key": "val"}

    def test_validation_error_fields(self):
        exc = ValidationError("bad qty", field="quantity", value=-1)
        assert exc.field == "quantity"
        assert exc.value == -1
        assert "bad qty" in str(exc)

    def test_api_error_status_code(self):
        exc = APIError("not found", status_code=404)
        assert exc.status_code == 404

    def test_retry_exhausted(self):
        inner = ValueError("boom")
        exc = RetryExhaustedError("gave up", attempts=3, last_error=inner)
        assert exc.attempts == 3
        assert exc.last_error is inner

    def test_exception_hierarchy(self):
        assert issubclass(ValidationError, PipelineError)
        assert issubclass(DataReadError,   PipelineError)
        assert issubclass(APIError,        PipelineError)


# ═══════════════════════════════════════════════════════════════════════════════
# Module 18 – Decorators
# ═══════════════════════════════════════════════════════════════════════════════

class TestDecorators:

    def test_timer_runs_function(self):
        @timer
        def add(a, b):
            return a + b

        assert add(2, 3) == 5

    def test_timer_with_label(self):
        @timer(label="MyTest")
        def add(a, b):
            return a + b

        assert add(10, 20) == 30

    def test_retry_succeeds_first_try(self):
        calls = []

        @retry(max_attempts=3, delay=0)
        def flaky():
            calls.append(1)
            return "ok"

        assert flaky() == "ok"
        assert len(calls) == 1

    def test_retry_succeeds_on_second_attempt(self):
        calls = []

        @retry(max_attempts=3, delay=0)
        def flaky():
            calls.append(1)
            if len(calls) < 2:
                raise IOError("temporary")
            return "recovered"

        assert flaky() == "recovered"
        assert len(calls) == 2

    def test_retry_raises_after_exhaustion(self):
        @retry(max_attempts=2, delay=0)
        def always_fails():
            raise ValueError("always")

        with pytest.raises(RetryExhaustedError) as exc_info:
            always_fails()
        assert exc_info.value.attempts == 2

    def test_retry_only_catches_specified_exceptions(self):
        @retry(max_attempts=3, delay=0, exceptions=(IOError,))
        def type_error():
            raise TypeError("not retried")

        with pytest.raises(TypeError):
            type_error()

    def test_pipeline_step_decorator_counts(self):
        @PipelineStep(name="Test", log_every=2)
        def gen(items):
            yield from items

        result = list(gen(range(5)))
        assert result == [0, 1, 2, 3, 4]

    def test_log_step_returns_value(self):
        @log_step(step_name="Greet")
        def greet(name):
            return f"Hello, {name}"

        assert greet("World") == "Hello, World"


# ═══════════════════════════════════════════════════════════════════════════════
# Module 15 – Abstract Base Classes
# ═══════════════════════════════════════════════════════════════════════════════

class TestAbstractBaseClasses:

    def test_datasource_cannot_be_instantiated(self):
        with pytest.raises(TypeError):
            DataSource({})  # type: ignore

    def test_datatransformer_cannot_be_instantiated(self):
        with pytest.raises(TypeError):
            DataTransformer()  # type: ignore

    def test_datawriter_cannot_be_instantiated(self):
        with pytest.raises(TypeError):
            DataWriter({})  # type: ignore

    def test_concrete_transformer_must_implement_transform(self):
        class Incomplete(DataTransformer):
            pass  # missing transform()

        with pytest.raises(TypeError):
            Incomplete()

    def test_pipe_operator_composes_transformers(self):
        class Double(DataTransformer):
            def transform(self, records):
                for r in records:
                    yield {**r, "qty": r["qty"] * 2}

        class AddOne(DataTransformer):
            def transform(self, records):
                for r in records:
                    yield {**r, "qty": r["qty"] + 1}

        composed = Double() | AddOne()
        result = list(composed.transform(iter([{"qty": 3}])))
        assert result == [{"qty": 7}]   # 3*2+1


# ═══════════════════════════════════════════════════════════════════════════════
# Module 16 – Generators / lazy processing
# ═══════════════════════════════════════════════════════════════════════════════

class TestGenerators:

    def test_csv_reader_is_generator(self, tmp_csv):
        """CSVReader.read() must return a generator (lazy, not a list)."""
        import inspect
        reader = CSVReader({"path": str(tmp_csv)})
        reader.connect()
        gen = reader.read()
        # Generators are iterators but not lists
        assert hasattr(gen, "__next__") or inspect.isgenerator(gen)
        reader.close()

    def test_csv_reader_yields_correct_count(self, tmp_csv):
        reader = CSVReader({"path": str(tmp_csv)})
        with reader:
            rows = list(reader.read())
        assert len(rows) == 10

    def test_csv_reader_missing_file_raises(self, tmp_path):
        reader = CSVReader({"path": str(tmp_path / "no_such_file.csv")})
        with pytest.raises(DataReadError):
            reader.connect()

    def test_mock_db_reader_yields_expected_count(self):
        db = MockDBReader({"total_rows": 50, "batch_size": 10})
        with db:
            rows = list(db.read())
        assert len(rows) == 50

    def test_cleaning_transformer_is_lazy(self, multi_stream):
        """CleaningTransformer.transform() must return a generator, not a list."""
        import inspect
        ct = CleaningTransformer()
        result = ct.transform(multi_stream)
        assert hasattr(result, "__next__") or inspect.isgenerator(result)


# ═══════════════════════════════════════════════════════════════════════════════
# Data transformation logic
# ═══════════════════════════════════════════════════════════════════════════════

class TestCleaningTransformer:

    def _run(self, records):
        ct = CleaningTransformer()
        return list(ct.transform(iter(records)))

    def test_valid_record_passes_through(self, valid_record):
        result = self._run([valid_record])
        assert len(result) == 1
        assert result[0]["order_id"] == "ORD-0000001"

    def test_negative_quantity_dropped(self, valid_record):
        valid_record["quantity"] = "-1"
        assert self._run([valid_record]) == []

    def test_zero_quantity_dropped(self, valid_record):
        valid_record["quantity"] = "0"
        assert self._run([valid_record]) == []

    def test_zero_price_dropped(self, valid_record):
        valid_record["unit_price"] = "0"
        assert self._run([valid_record]) == []

    def test_bad_date_dropped(self, valid_record):
        valid_record["order_date"] = "bad-date"
        assert self._run([valid_record]) == []

    def test_discount_over_100_percent_dropped(self, valid_record):
        valid_record["discount_pct"] = "1.5"
        assert self._run([valid_record]) == []

    def test_missing_customer_id_dropped(self, valid_record):
        valid_record["customer_id"] = ""
        assert self._run([valid_record]) == []

    def test_duplicate_order_ids_deduplicated(self):
        r1 = _make_valid_record(order_id="ORD-0000001")
        r2 = _make_valid_record(order_id="ORD-0000001")  # exact duplicate
        result = self._run([r1, r2])
        assert len(result) == 1

    def test_derived_date_fields_added(self, valid_record):
        result = self._run([valid_record])
        assert result[0]["order_year"]    == 2024
        assert result[0]["order_month"]   == 3
        assert result[0]["order_quarter"] == "Q1"

    def test_multiple_valid_records_all_pass(self):
        records = [_make_valid_record(order_id=f"ORD-{i:07d}") for i in range(10)]
        result = self._run(records)
        assert len(result) == 10

    def test_mixed_valid_invalid_records(self):
        records = [
            _make_valid_record(order_id="ORD-0000001"),
            _make_valid_record(order_id="ORD-0000002", quantity="-5"),   # invalid
            _make_valid_record(order_id="ORD-0000003"),
            _make_valid_record(order_id="ORD-0000004", unit_price="0"),  # invalid
            _make_valid_record(order_id="ORD-0000005"),
        ]
        result = self._run(records)
        assert len(result) == 3


class TestEnrichmentTransformer:

    def _run(self, records, meta=None):
        meta = meta or {"Electronics": {"avg_cost_pct": 0.55, "tier": "Premium"}}
        ct = CleaningTransformer()
        et = EnrichmentTransformer(category_metadata=meta)
        stream = et.transform(ct.transform(iter(records)))
        return list(stream)

    def test_financial_columns_added(self, valid_record):
        result = self._run([valid_record])
        r = result[0]
        assert "gross_revenue"   in r
        assert "net_revenue"     in r
        assert "gross_margin"    in r
        assert "discount_amount" in r
        assert "margin_pct"      in r

    def test_net_revenue_calculation(self, valid_record):
        # qty=2, price=899.99, discount=10% → gross=1799.98, disc=179.998, net≈1619.98
        result = self._run([valid_record])
        r = result[0]
        expected_gross = round(2 * 899.99, 2)
        expected_disc  = round(expected_gross * 0.10, 2)
        expected_net   = round(expected_gross - expected_disc, 2)
        assert r["gross_revenue"]   == expected_gross
        assert r["discount_amount"] == expected_disc
        assert r["net_revenue"]     == expected_net

    def test_category_tier_from_metadata(self, valid_record):
        result = self._run([valid_record])
        assert result[0]["category_tier"] == "Premium"

    def test_high_value_flag(self, valid_record):
        # 2 * 899.99 * 0.9 = 1619.98 → is_high_value True
        result = self._run([valid_record])
        assert result[0]["is_high_value"] is True

    def test_low_value_flag(self):
        rec = _make_valid_record(unit_price="10.00", quantity="1", discount_pct="0")
        result = self._run([rec])
        assert result[0]["is_high_value"] is False


class TestAggregationTransformer:

    def _run(self, records):
        ct  = CleaningTransformer()
        et  = EnrichmentTransformer(category_metadata={})
        at  = AggregationTransformer()
        return list(at.transform(et.transform(ct.transform(iter(records)))))

    def test_groups_by_region_and_category(self):
        records = [
            _make_valid_record(order_id="ORD-0000001", region="East", category="Electronics"),
            _make_valid_record(order_id="ORD-0000002", region="East", category="Electronics"),
            _make_valid_record(order_id="ORD-0000003", region="West", category="Books"),
        ]
        result = self._run(records)
        keys = {(r["region"], r["category"]) for r in result}
        assert ("East", "Electronics") in keys
        assert ("West", "Books") in keys

    def test_order_count_aggregated(self):
        records = [
            _make_valid_record(order_id=f"ORD-{i:07d}", region="North", category="Sports")
            for i in range(5)
        ]
        result = self._run(records)
        north_sports = next(
            r for r in result
            if r["region"] == "North" and r["category"] == "Sports"
        )
        assert north_sports["order_count"] == 5

    def test_returned_orders_counted(self):
        records = [
            _make_valid_record(order_id="ORD-0000001", status="Returned", region="South", category="Clothing"),
            _make_valid_record(order_id="ORD-0000002", status="Delivered", region="South", category="Clothing"),
        ]
        result = self._run(records)
        bucket = next(r for r in result if r["region"] == "South")
        assert bucket["returned_orders"] == 1
        assert bucket["return_rate_pct"] == 50.0


# ═══════════════════════════════════════════════════════════════════════════════
# Module 17 – Config loading
# ═══════════════════════════════════════════════════════════════════════════════

class TestConfigLoader:

    @pytest.fixture
    def config_dir(self, tmp_path):
        cfg = {
            "pipeline": {"name": "Test Pipeline"},
            "source":   {"type": "csv", "path": "./data/sales_raw.csv"},
            "output":   {"dir": "./output"},
        }
        import yaml
        (tmp_path / "dev.yaml").write_text(yaml.dump(cfg))
        return tmp_path

    def test_load_dev_config(self, config_dir):
        cfg = load_config(env="dev", config_dir=config_dir)
        assert cfg["pipeline"]["name"] == "Test Pipeline"
        assert cfg["_env"] == "dev"

    def test_missing_env_file_raises(self, tmp_path):
        with pytest.raises(ConfigError):
            load_config(env="staging", config_dir=tmp_path)

    def test_validate_config_passes(self, config_dir):
        cfg = load_config(env="dev", config_dir=config_dir)
        validate_config(cfg)  # should not raise

    def test_validate_config_missing_key(self, config_dir):
        cfg = load_config(env="dev", config_dir=config_dir)
        del cfg["output"]
        with pytest.raises(ConfigError):
            validate_config(cfg)

    def test_merge_configs(self):
        base     = {"a": 1, "b": {"x": 10, "y": 20}}
        override = {"b": {"y": 99, "z": 30}, "c": 3}
        merged   = merge_configs(base, override)
        assert merged["a"]    == 1
        assert merged["b"]["x"] == 10
        assert merged["b"]["y"] == 99   # overridden
        assert merged["b"]["z"] == 30   # new key
        assert merged["c"]    == 3


# ═══════════════════════════════════════════════════════════════════════════════
# Module 19 – Async metadata fetch
# ═══════════════════════════════════════════════════════════════════════════════

class TestAsyncMetadataFetch:

    def test_fetch_known_category(self):
        from main import fetch_all_category_metadata
        result = asyncio.run(fetch_all_category_metadata(["Electronics"], max_concurrent=2))
        assert "Electronics" in result
        assert "avg_cost_pct" in result["Electronics"]

    def test_fetch_multiple_categories_concurrently(self):
        from main import fetch_all_category_metadata
        cats = ["Electronics", "Clothing", "Books"]
        result = asyncio.run(fetch_all_category_metadata(cats, max_concurrent=3))
        assert len(result) == len(cats)

    def test_unknown_category_skipped_not_raised(self):
        from main import fetch_all_category_metadata
        # Unknown categories should be skipped (logged as warning), not crash
        result = asyncio.run(
            fetch_all_category_metadata(["Electronics", "NONEXISTENT"], max_concurrent=2)
        )
        assert "Electronics" in result
        assert "NONEXISTENT" not in result


# ═══════════════════════════════════════════════════════════════════════════════
# Writers
# ═══════════════════════════════════════════════════════════════════════════════

class TestCSVWriter:

    def test_writes_correct_row_count(self, tmp_path):
        path = tmp_path / "out.csv"
        rows = [_make_valid_record(order_id=f"ORD-{i:07d}") for i in range(5)]
        with CSVWriter({"path": str(path)}) as w:
            n = w.write(iter(rows))
        assert n == 5
        assert path.exists()

    def test_output_is_readable_csv(self, tmp_path):
        path = tmp_path / "out.csv"
        rows = [_make_valid_record(order_id=f"ORD-{i:07d}") for i in range(3)]
        with CSVWriter({"path": str(path)}) as w:
            w.write(iter(rows))
        with open(path) as f:
            result = list(csv.DictReader(f))
        assert len(result) == 3
        assert result[0]["order_id"] == "ORD-0000000"


class TestJSONWriter:

    def test_writes_valid_json(self, tmp_path):
        path = tmp_path / "out.json"
        data = [{"region": "East", "net_revenue": 1000.0}]
        with JSONWriter({"path": str(path)}) as w:
            n = w.write(iter(data))
        assert n == 1
        with open(path) as f:
            loaded = json.load(f)
        assert loaded[0]["region"] == "East"


# ═══════════════════════════════════════════════════════════════════════════════
# End-to-end – full mini pipeline
# ═══════════════════════════════════════════════════════════════════════════════

class TestEndToEndMiniPipeline:

    def test_full_pipeline_clean_and_write(self, tmp_csv, tmp_path):
        out_path = tmp_path / "cleaned.csv"
        pipeline = Pipeline(
            source       = CSVReader({"path": str(tmp_csv)}),
            transformers = [CleaningTransformer(), EnrichmentTransformer()],
            writer       = CSVWriter({"path": str(out_path)}),
            name         = "MiniTest",
        )
        summary = pipeline.run()
        assert summary["rows_written"] == 10
        assert out_path.exists()

    def test_pipeline_summary_dict_keys(self, tmp_csv, tmp_path):
        out_path = tmp_path / "result.csv"
        pipeline = Pipeline(
            source       = CSVReader({"path": str(tmp_csv)}),
            transformers = [CleaningTransformer()],
            writer       = CSVWriter({"path": str(out_path)}),
        )
        summary = pipeline.run()
        assert "rows_written" in summary
        assert "elapsed_s"    in summary
        assert "pipeline"     in summary
