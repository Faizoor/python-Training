"""
pipeline/decorators.py
Reusable decorators and a class decorator for pipeline stages.
Module 18 – Function decorators, class decorators, __call__.
"""

import time
import logging
import functools
import random

from pipeline.exceptions import RetryExhaustedError

logger = logging.getLogger(__name__)


# ── 1. @timer – measures execution wall-time ───────────────────────────────────

def timer(func=None, *, label: str | None = None):
    """
    Decorator that logs how long a function takes.

    Can be used with or without arguments:
        @timer
        @timer(label="Read CSV")
    """
    if func is None:
        # Called with arguments: @timer(label="…")
        return functools.partial(timer, label=label)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        name = label or func.__qualname__
        start = time.perf_counter()
        logger.info("⏱  [%s] started", name)
        try:
            result = func(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - start
            logger.info("⏱  [%s] finished in %.3f s", name, elapsed)
        return result

    return wrapper


# ── 2. @retry – exponential-backoff retry ─────────────────────────────────────

def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    jitter: float = 0.2,
    exceptions: tuple = (Exception,),
):
    """
    Retry decorator with exponential back-off + random jitter.

    Args:
        max_attempts: Total attempts (including the first try).
        delay:        Initial wait in seconds.
        backoff:      Multiplier applied after each failure.
        jitter:       Max random seconds added to each wait.
        exceptions:   Exception types that trigger a retry.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            wait = delay
            last_exc: Exception | None = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        break
                    sleep_time = wait + random.uniform(0, jitter)
                    logger.warning(
                        "🔁 [%s] attempt %d/%d failed (%s). Retrying in %.2f s …",
                        func.__qualname__, attempt, max_attempts, exc, sleep_time,
                    )
                    time.sleep(sleep_time)
                    wait *= backoff

            raise RetryExhaustedError(
                f"{func.__qualname__} failed after {max_attempts} attempts",
                attempts=max_attempts,
                last_error=last_exc,
            ) from last_exc

        return wrapper
    return decorator


# ── 3. @log_step – structured step logging ────────────────────────────────────

def log_step(func=None, *, step_name: str | None = None):
    """
    Logs entry / exit of a pipeline step with a structured banner.
    Can be combined with @timer.
    """
    if func is None:
        return functools.partial(log_step, step_name=step_name)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        name = step_name or func.__qualname__
        logger.info("=" * 55)
        logger.info("▶  STEP  : %s", name)
        result = func(*args, **kwargs)
        logger.info("✔  DONE  : %s", name)
        logger.info("=" * 55)
        return result

    return wrapper


# ── 4. @validate_schema – guards a function's first positional arg ─────────────

def validate_schema(required_fields: list[str]):
    """
    Decorator that ensures every record (dict) yielded by a generator
    contains the required fields before passing it downstream.
    Skips (and warns about) records that fail the check.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for record in func(*args, **kwargs):
                missing = [f for f in required_fields if f not in record]
                if missing:
                    logger.warning(
                        "⚠  Schema mismatch – missing fields %s in record %s",
                        missing, record.get("order_id", "?"),
                    )
                    continue
                yield record
        return wrapper
    return decorator


# ── 5. PipelineStep – class decorator with __call__ ───────────────────────────

class PipelineStep:
    """
    Class decorator that wraps a generator function as a named, timed pipeline step.

    Usage:
        @PipelineStep(name="Filter Bad Records")
        def filter_records(records):
            for r in records:
                if r["quantity"] > 0:
                    yield r
    """

    def __init__(self, name: str | None = None, log_every: int = 10_000):
        self.name = name
        self.log_every = log_every

    def __call__(self, func):
        name = self.name or func.__qualname__
        log_every = self.log_every

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger.info("🔧  PipelineStep [%s] – starting", name)
            count = 0
            start = time.perf_counter()
            for item in func(*args, **kwargs):
                count += 1
                if count % log_every == 0:
                    logger.debug(
                        "  … [%s] processed %d records (%.1f s so far)",
                        name, count, time.perf_counter() - start,
                    )
                yield item
            elapsed = time.perf_counter() - start
            logger.info("✅  PipelineStep [%s] – %d records in %.3f s", name, count, elapsed)

        return wrapper
