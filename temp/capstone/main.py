"""
main.py  ──  Advanced E-Commerce Sales Analytics Pipeline
──────────────────────────────────────────────────────────────────────────────
Demonstrates every concept from Modules 15-20:

  Module 15 – ABCs (DataSource, DataTransformer, DataWriter), super(),
               method overriding, entity class modelling
  Module 16 – Generator pipeline: source → clean → enrich → aggregate
               (constant memory regardless of file size)
  Module 17 – YAML config + env switching (dev / prod) via PIPELINE_ENV
  Module 18 – @timer, @retry, @log_step, @PipelineStep class decorator
  Module 19 – asyncio (category metadata "API"), concurrent.futures
               (parallel chunk validation), multiprocessing comparison
  Module 20 – Custom exception hierarchy, retry patterns, structured
               logging (file + console), pytest-compatible helpers

Usage:
    python main.py                    # uses dev config
    PIPELINE_ENV=prod python main.py  # uses prod config

Output (in output/<env>/):
    cleaned_sales.csv     – every valid order enriched with financials
    aggregated_summary.json – grouped by region / category / quarter
    pipeline_report.md    – human-readable Markdown summary
──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

# ── local imports ──────────────────────────────────────────────────────────────
from pipeline.base import Pipeline
from pipeline.decorators import log_step, retry, timer
from pipeline.exceptions import (
    APIError,
    ConfigError,
    PipelineError,
    RetryExhaustedError,
)
from pipeline.readers import CSVReader
from pipeline.transformers import (
    AggregationTransformer,
    CleaningTransformer,
    EnrichmentTransformer,
)
from pipeline.writers import CSVWriter, JSONWriter, SummaryWriter
from utils.config_loader import load_config, validate_config
from utils.logger import setup_logging

logger = logging.getLogger(__name__)

# ── Category metadata served by a mock async "REST API" ───────────────────────
# In production this would be an aiohttp / httpx call to a real endpoint.
# Here we use asyncio.sleep() to simulate realistic network latency and
# demonstrate Module 19 async patterns without requiring internet access.

_MOCK_API_DATA: dict[str, dict] = {
    "Electronics":    {"avg_cost_pct": 0.55, "tier": "Premium"},
    "Clothing":       {"avg_cost_pct": 0.35, "tier": "Standard"},
    "Home & Kitchen": {"avg_cost_pct": 0.40, "tier": "Standard"},
    "Books":          {"avg_cost_pct": 0.25, "tier": "Value"},
    "Sports":         {"avg_cost_pct": 0.38, "tier": "Standard"},
}


# ── Module 19: asyncio helpers ─────────────────────────────────────────────────

async def _fetch_category_meta(
    session_semaphore: asyncio.Semaphore,
    category: str,
    timeout: float = 5.0,
) -> tuple[str, dict]:
    """
    Coroutine that fetches metadata for a single category.
    Simulates I/O-bound HTTP with asyncio.sleep().

    In real pipelines: replace with aiohttp.ClientSession.get(url).
    """
    async with session_semaphore:
        latency = 0.05 + 0.15 * (hash(category) % 7) / 6  # 50–200 ms
        await asyncio.sleep(latency)

        data = _MOCK_API_DATA.get(category)
        if data is None:
            raise APIError(f"Unknown category: {category!r}", status_code=404)

        logger.debug("  async: fetched metadata for %r (%.0f ms)", category, latency * 1000)
        return category, data


async def fetch_all_category_metadata(
    categories: list[str],
    max_concurrent: int = 5,
    timeout: float = 5.0,
) -> dict[str, dict]:
    """
    Module 19: fan-out all category requests concurrently using asyncio.gather().

    Key point: all N HTTP calls fly in parallel (not sequential).
    Semaphore limits concurrent requests to `max_concurrent`.
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = [
        _fetch_category_meta(semaphore, cat, timeout)
        for cat in categories
    ]

    logger.info("⚡ async: fetching metadata for %d categories concurrently …", len(tasks))
    start = time.perf_counter()

    results = await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.perf_counter() - start
    logger.info("⚡ async: all metadata fetched in %.3f s (sequential would be ~%.3f s)",
                elapsed,
                sum(0.05 + 0.15 * (hash(c) % 7) / 6 for c in categories))

    metadata: dict[str, dict] = {}
    for res in results:
        if isinstance(res, Exception):
            logger.warning("async fetch error (ignored): %s", res)
        else:
            cat, data = res
            metadata[cat] = data

    return metadata


# ── Module 19: concurrent.futures – parallel chunk validation ─────────────────

def _validate_chunk(chunk: list[dict]) -> dict:
    """
    Worker function executed in a subprocess (ProcessPoolExecutor).
    Counts valid / invalid / duplicate records without I/O.

    Pure function → safe for multiprocessing (no shared state, picklable).
    Module 19: CPU-bound work off-loaded to separate processes.
    """
    seen: set[str] = set()
    valid = invalid = duplicates = 0

    for row in chunk:
        oid = str(row.get("order_id") or "")
        if not oid:
            invalid += 1
            continue
        if oid in seen:
            duplicates += 1
            continue
        seen.add(oid)
        try:
            qty = int(float(row.get("quantity", 0)))
            price = float(row.get("unit_price", 0))
            disc = float(row.get("discount_pct", 0))
            if qty <= 0 or price <= 0 or disc > 1.0 or row.get("order_date") == "bad-date":
                invalid += 1
            else:
                valid += 1
        except (ValueError, TypeError):
            invalid += 1

    return {"valid": valid, "invalid": invalid, "duplicates": duplicates}


@timer(label="parallel_validate")
@log_step(step_name="Parallel Pre-validation (concurrent.futures)")
def parallel_validate(
    csv_path: str,
    chunk_size: int = 20_000,
    max_workers: int = 4,
) -> dict:
    """
    Module 19 – ProcessPoolExecutor: Splits the CSV into chunks and
    validates each chunk in a separate process.

    Returns aggregate counts (valid, invalid, duplicates).
    """
    # Read all rows first – only for pre-validation scan (not the main pipeline)
    all_rows: list[dict] = []
    with open(csv_path, newline="") as f:
        all_rows = list(csv.DictReader(f))

    total = len(all_rows)
    chunks = [all_rows[i : i + chunk_size] for i in range(0, total, chunk_size)]
    logger.info(
        "🔀  Parallel validation: %d rows → %d chunks, %d workers",
        total, len(chunks), max_workers,
    )

    totals = {"valid": 0, "invalid": 0, "duplicates": 0}
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_validate_chunk, chunk): i
                   for i, chunk in enumerate(chunks)}
        for future in as_completed(futures):
            chunk_id = futures[future]
            try:
                result = future.result()
                for k in totals:
                    totals[k] += result[k]
                logger.debug("  chunk %d validated: %s", chunk_id, result)
            except Exception as exc:
                logger.error("  chunk %d error: %s", chunk_id, exc)

    logger.info(
        "✔  Parallel validation complete: valid=%d, invalid=%d, duplicates=%d",
        totals["valid"], totals["invalid"], totals["duplicates"],
    )
    return totals


# ── Module 18: @retry applied to a flaky pipeline step ────────────────────────

@retry(max_attempts=3, delay=0.5, backoff=2.0, exceptions=(IOError, OSError))
def safe_mkdir(path: str) -> None:
    """Demonstrates @retry on infrastructure operations."""
    os.makedirs(path, exist_ok=True)


# ── Main orchestration logic ───────────────────────────────────────────────────

@timer(label="full_pipeline_run")
@log_step(step_name="Advanced Sales Analytics Pipeline")
def run_pipeline(config: dict) -> dict:
    """
    Orchestrates the entire advanced pipeline:
      1. Async category metadata fetch (Module 19 asyncio)
      2. Parallel pre-validation (Module 19 concurrent.futures)
      3. Full streaming pipeline:
            CSVReader → CleaningTransformer → EnrichmentTransformer →
            CSVWriter (cleaned rows)
      4. Second pass: aggregation → JSONWriter + SummaryWriter (report)
    """
    src_cfg  = config["source"]
    out_cfg  = config["output"]
    par_cfg  = config.get("parallel", {})
    async_cfg = config.get("async", {})
    pipe_name = config["pipeline"]["name"]

    # Resolve output directory relative to script location
    base_dir  = Path(__file__).parent
    out_dir   = base_dir / out_cfg["dir"]
    safe_mkdir(str(out_dir))

    csv_path  = str(base_dir / src_cfg["path"])
    max_workers  = par_cfg.get("max_workers", 2)
    chunk_size   = par_cfg.get("chunk_size", 10_000)
    max_concurrent = async_cfg.get("max_concurrent", 5)

    run_start = time.perf_counter()

    # ── Step 1: Async fetch category metadata ──────────────────────────────────
    # Module 19: asyncio.run() + concurrent coroutines
    logger.info("")
    logger.info("STEP 1 ▶  Async category metadata fetch")
    categories = list(_MOCK_API_DATA.keys())
    category_meta = asyncio.run(
        fetch_all_category_metadata(categories, max_concurrent=max_concurrent)
    )
    logger.info("  Fetched metadata for: %s", list(category_meta.keys()))

    # ── Step 2: Parallel pre-validation ───────────────────────────────────────
    # Module 19: ProcessPoolExecutor
    logger.info("")
    logger.info("STEP 2 ▶  Parallel pre-validation")
    validation_summary = parallel_validate(
        csv_path, chunk_size=chunk_size, max_workers=max_workers
    )

    # ── Step 3: Streaming clean + enrich  →  cleaned CSV ──────────────────────
    # Module 16: generator pipeline (constant memory)
    # Module 15: ABCs, Pipeline orchestrator
    logger.info("")
    logger.info("STEP 3 ▶  Streaming clean → enrich → write cleaned CSV")
    clean_path = str(out_dir / out_cfg["cleaned_csv"])

    cleaning_tx   = CleaningTransformer()
    enrichment_tx = EnrichmentTransformer(category_metadata=category_meta)

    # Use pipe operator  (Module 15: _ComposedTransformer)
    combined_tx = cleaning_tx | enrichment_tx

    csv_pipeline = Pipeline(
        source       = CSVReader({"path": csv_path, "name": "SalesCSV"}),
        transformers = [combined_tx],
        writer       = CSVWriter({"path": clean_path}),
        name         = f"{pipe_name} – Clean+Enrich",
    )
    clean_result = csv_pipeline.run()

    # ── Step 4: Aggregation pass  →  JSON + Markdown report ───────────────────
    # Re-reads the cleaned CSV (so aggregation is also streamed)
    logger.info("")
    logger.info("STEP 4 ▶  Aggregation → JSON + Markdown report")

    agg_path     = str(out_dir / out_cfg["summary_json"])
    report_path  = str(out_dir / out_cfg["report_md"])
    run_elapsed  = round(time.perf_counter() - run_start, 3)

    agg_pipeline = Pipeline(
        source       = CSVReader({"path": clean_path, "name": "CleanedCSV"}),
        transformers = [AggregationTransformer()],
        writer       = JSONWriter({"path": agg_path}),
        name         = f"{pipe_name} – Aggregate",
    )
    agg_result = agg_pipeline.run()

    # Load aggregated records to feed SummaryWriter
    with open(agg_path) as f:
        agg_records = json.load(f)

    run_info = {
        "Environment":         config["_env"],
        "Elapsed (s)":         run_elapsed,
        "Raw rows":            f"{validation_summary['valid'] + validation_summary['invalid'] + validation_summary['duplicates']:,}",
        "Clean rows written":  f"{clean_result['rows_written']:,}",
        "Dropped (invalid)":   f"{validation_summary['invalid']:,}",
        "Dropped (duplicate)": f"{validation_summary['duplicates']:,}",
        "Aggregation buckets": f"{agg_result['rows_written']:,}",
    }

    with SummaryWriter({"path": report_path, "run_info": run_info}) as sw:
        rows_written = sw.write(iter(agg_records))

    logger.info("")
    logger.info("=" * 60)
    logger.info("  PIPELINE COMPLETE")
    logger.info("=" * 60)
    for k, v in run_info.items():
        logger.info("  %-28s %s", k + ":", v)
    logger.info("  Cleaned CSV  →  %s", clean_path)
    logger.info("  Summary JSON →  %s", agg_path)
    logger.info("  Report MD    →  %s", report_path)
    logger.info("=" * 60)

    return {**run_info, "outputs": [clean_path, agg_path, report_path]}


# ── CLI entry-point ────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Advanced Sales Analytics Pipeline")
    p.add_argument(
        "--env", "-e",
        default=None,
        help="Config environment: dev | prod  (default: PIPELINE_ENV env var or 'dev')",
    )
    p.add_argument(
        "--config-dir", "-c",
        default=None,
        help="Directory containing <env>.yaml files (default: ./config)",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    # ── Module 17: load + validate config ────────────────────────────────────
    try:
        config = load_config(env=args.env, config_dir=args.config_dir)
        validate_config(config)
    except ConfigError as exc:
        print(f"[FATAL] Config error: {exc}", file=sys.stderr)
        sys.exit(1)

    # ── Module 20: setup structured logging ──────────────────────────────────
    log_cfg = config.get("logging", {})
    setup_logging(
        level             = log_cfg.get("level", "INFO"),
        log_dir           = log_cfg.get("log_dir", "./logs"),
        log_filename      = log_cfg.get("log_file", "pipeline.log"),
        enable_file_handler = True,
    )

    logger.info("🚀  Starting pipeline  |  env=%s", config["_env"])

    # ── Module 20: top-level exception handling ───────────────────────────────
    try:
        result = run_pipeline(config)
        logger.info("🏁  Pipeline finished successfully: %s", result)
    except RetryExhaustedError as exc:
        logger.critical("💥  Retry exhausted: %s  (last error: %s)", exc, exc.last_error)
        sys.exit(2)
    except PipelineError as exc:
        logger.critical("💥  Pipeline error: %s", exc)
        sys.exit(2)
    except KeyboardInterrupt:
        logger.warning("⚠   Interrupted by user")
        sys.exit(130)
    except Exception as exc:
        logger.exception("💥  Unexpected error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
