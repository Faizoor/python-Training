import asyncio
import random
import time


async def fetch(url: str) -> dict:
    delay = random.uniform(0.05, 0.25)
    await asyncio.sleep(delay)
    return {"url": url, "status": 200, "delay": delay}


async def main(urls):
    start = time.perf_counter()
    results = []
    for u in urls:
        result = await fetch(u)
        results.append(result)
    elapsed = time.perf_counter() - start
    print(f"Fetched {len(results)} urls in {elapsed:.3f}s")
    return results


if __name__ == "__main__":
    urls = [f"https://example.com/resource/{i}" for i in range(50)]
    results = asyncio.run(main(urls))
    print(results[:3])
