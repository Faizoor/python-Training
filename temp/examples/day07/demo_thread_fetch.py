import asyncio
import random
import time
from concurrent.futures import ThreadPoolExecutor
import threading


def fetch(url: str) -> dict:
    delay = random.uniform(0.05, 0.25)
    time.sleep(delay)
    return {"url": url, "status": 200, "delay": delay}


def main(urls):
    start = time.perf_counter()
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch, u) for u in urls]
        results = [f.result() for f in futures]
    elapsed = time.perf_counter() - start
    print(f"Fetched {len(results)} urls in {elapsed:.3f}s")
    return results


if __name__ == "__main__":
    urls = [f"https://example.com/resource/{i}" for i in range(50)]
    results = main(urls)
    print(results[:3])
