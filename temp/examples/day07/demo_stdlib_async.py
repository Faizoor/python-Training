# demo_stdlib_async.py
"""
Demonstrates practical asyncio use cases from the Python standard library:
  - Grabbing web pages concurrently using asyncio + urllib.request in a thread pool
  - Sending email notifications with smtplib (simulated, no real SMTP server needed)
  - Using math module for ETL metric calculations (e.g., log scaling, rounding)
  - Using random module for jitter, sampling, and test data generation

These utilities are commonly combined in real pipeline monitoring and notification flows.
"""

import asyncio
import math
import random
import smtplib
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from email.mime.text import MIMEText


# ---------------------------------------------------------------------------
# Part 1: Grabbing web pages concurrently
# ---------------------------------------------------------------------------
# urllib.request.urlopen is blocking, so we run it in a thread pool executor
# to keep the asyncio event loop unblocked.

def _fetch_url_sync(url: str) -> dict:
    """Synchronous HTTP GET — runs in a thread pool to avoid blocking the event loop."""
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = resp.read(512)  # read first 512 bytes only
            return {"url": url, "status": resp.status, "preview": body[:80].decode("utf-8", errors="replace")}
    except Exception as exc:
        return {"url": url, "status": "error", "error": str(exc)}


async def fetch_pages_concurrently(urls: list[str], workers: int = 5) -> list[dict]:
    """Fetch multiple web pages concurrently using asyncio + ThreadPoolExecutor.

    Because urllib is blocking I/O (not a native coroutine), we offload each
    request to a thread, but orchestrate them via the asyncio event loop so
    all requests are in-flight at the same time.
    """
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        tasks = [loop.run_in_executor(executor, _fetch_url_sync, url) for url in urls]
        results = await asyncio.gather(*tasks)
    return results


# ---------------------------------------------------------------------------
# Part 2: Sending email notifications (simulated)
# ---------------------------------------------------------------------------
# Real pipelines send alert emails when critical errors occur (e.g., SLA breach,
# failed loads). We simulate the send here without a real SMTP server.

def send_pipeline_alert(
    subject: str,
    body: str,
    to_addr: str = "ops-team@example.com",
    from_addr: str = "pipeline@example.com",
    smtp_host: str = "localhost",
    smtp_port: int = 1025,
    dry_run: bool = True,
) -> bool:
    """Send (or simulate) an email alert for a pipeline event.

    Args:
        subject:   Email subject line.
        body:      Plain-text email body.
        to_addr:   Recipient address.
        from_addr: Sender address.
        smtp_host: SMTP server hostname.
        smtp_port: SMTP server port (1025 = local debug server).
        dry_run:   When True, log the email instead of actually sending.

    Returns True on success, False on failure.
    """
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr

    if dry_run:
        print(f"[DRY RUN] Would send email:\n  To: {to_addr}\n  Subject: {subject}\n  Body: {body}")
        return True

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=5) as server:
            server.sendmail(from_addr, [to_addr], msg.as_string())
        print(f"Email sent to {to_addr}: {subject}")
        return True
    except Exception as exc:
        print(f"Failed to send email: {exc}")
        return False


# ---------------------------------------------------------------------------
# Part 3: math module — ETL metric helpers
# ---------------------------------------------------------------------------
# The math module provides functions useful for data quality scoring,
# backoff calculations, and normalizing metrics.

def compute_quality_score(null_count: int, total_count: int) -> float:
    """Return a log-scaled quality score in [0, 100].

    Uses math.log1p so that a small number of nulls causes a gentle penalty
    rather than a hard cliff. Score of 100 means zero nulls.

    Args:
        null_count:  Number of null/missing values in the batch.
        total_count: Total number of records in the batch.
    """
    if total_count == 0:
        return 0.0
    null_ratio = null_count / total_count
    # log1p(0) = 0  →  score = 100 (perfect)
    # log1p(1) ≈ 0.69  →  score ≈ 31 (all nulls)
    penalty = math.log1p(null_ratio) / math.log1p(1)  # normalise to [0, 1]
    return round((1 - penalty) * 100, 2)


def exponential_backoff_delay(attempt: int, base: float = 0.5, cap: float = 30.0) -> float:
    """Return the delay (seconds) for a given retry attempt using exponential backoff.

    Formula: min(cap, base * 2^(attempt-1))

    Args:
        attempt: Current attempt number (1-based).
        base:    Base delay in seconds.
        cap:     Maximum allowed delay in seconds.
    """
    return min(cap, base * math.pow(2, attempt - 1))


# ---------------------------------------------------------------------------
# Part 4: random module — sampling, jitter, test data generation
# ---------------------------------------------------------------------------
# random is used in pipelines for: jitter in backoff, stratified sampling,
# synthetic test data, and chaos/fault injection in testing.

def generate_sample_records(n: int = 10, seed: int | None = 42) -> list[dict]:
    """Generate reproducible synthetic pipeline records for testing.

    Args:
        n:    Number of records to generate.
        seed: Random seed for reproducibility (None = non-deterministic).
    """
    rng = random.Random(seed)
    records = []
    for i in range(n):
        records.append({
            "id": i + 1,
            "value": round(rng.gauss(mu=100.0, sigma=15.0), 2),   # normal distribution
            "category": rng.choice(["A", "B", "C"]),
            "is_valid": rng.random() > 0.1,                        # 90% valid
        })
    return records


def stratified_sample(records: list[dict], fraction: float = 0.3, seed: int = 0) -> list[dict]:
    """Return a reproducible random sample of the given fraction of records.

    Args:
        records:  Source list of records.
        fraction: Proportion to sample (0 < fraction <= 1).
        seed:     Random seed for reproducibility.
    """
    rng = random.Random(seed)
    k = max(1, math.ceil(len(records) * fraction))
    return rng.sample(records, k)


def add_jitter(delay: float, jitter_pct: float = 0.2) -> float:
    """Add random jitter to a delay value to avoid thundering-herd retries.

    Args:
        delay:      Base delay in seconds.
        jitter_pct: Maximum jitter as a fraction of the delay (e.g., 0.2 = ±20%).
    """
    return delay * (1 + random.uniform(-jitter_pct, jitter_pct))


# ---------------------------------------------------------------------------
# Main: exercise all four parts
# ---------------------------------------------------------------------------
async def main():
    print("=" * 60)
    print("Part 1 — Concurrent web page fetching")
    print("=" * 60)
    urls = [
        "https://httpbin.org/get",
        "https://httpbin.org/uuid",
        "https://httpbin.org/ip",
        "https://example.com",
        "https://httpbin.org/status/404",
    ]
    start = time.perf_counter()
    pages = await fetch_pages_concurrently(urls)
    elapsed = time.perf_counter() - start
    for p in pages:
        print(f"  {p['url']!r:45s}  status={p['status']}")
    print(f"  → All {len(pages)} fetches completed in {elapsed:.2f}s (concurrently)\n")

    print("=" * 60)
    print("Part 2 — Email alert simulation")
    print("=" * 60)
    send_pipeline_alert(
        subject="[ALERT] Batch 42 SLA Breach",
        body="Batch 42 exceeded 60-minute SLA. Records processed: 0/5000.",
        dry_run=True,
    )
    print()

    print("=" * 60)
    print("Part 3 — math: quality scores and backoff delays")
    print("=" * 60)
    for null_count in [0, 50, 200, 1000]:
        score = compute_quality_score(null_count, total_count=1000)
        print(f"  nulls={null_count:4d}/1000  quality_score={score:6.2f}")
    print()
    print("  Exponential backoff delays (base=0.5s, cap=30s):")
    for attempt in range(1, 8):
        raw = exponential_backoff_delay(attempt)
        jittered = add_jitter(raw)
        print(f"    attempt {attempt}: base={raw:6.2f}s  +jitter={jittered:.2f}s")
    print()

    print("=" * 60)
    print("Part 4 — random: synthetic records and stratified sampling")
    print("=" * 60)
    records = generate_sample_records(n=10, seed=42)
    print("  Generated records:")
    for r in records:
        print(f"    {r}")
    sampled = stratified_sample(records, fraction=0.3)
    print(f"\n  30% stratified sample ({len(sampled)} records): {[r['id'] for r in sampled]}")


if __name__ == "__main__":
    asyncio.run(main())
