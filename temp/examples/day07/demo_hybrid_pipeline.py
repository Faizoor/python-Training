import asyncio
import random
import time
from concurrent.futures import ProcessPoolExecutor


async def fetch_item(i):
    await asyncio.sleep(random.uniform(0.01, 0.05))
    return {"id": i, "value": i * 100}


def cpu_transform(item):
    # Simulate heavy calculation
    s = 0
    for _ in range(20000):
        s += (_ * (_ % (item["id"] + 1))) % 7
    return {"id": item["id"], "transformed": item["value"] + s}


async def main():
    # Stage 1: async fetch
    items = await asyncio.gather(*(fetch_item(i) for i in range(20)))

    # Stage 2: CPU-bound transformation in a process pool
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=4) as pool:
        tasks = [loop.run_in_executor(pool, cpu_transform, it) for it in items]
        transformed = await asyncio.gather(*tasks)

    # Stage 3: aggregation
    total = sum(x["transformed"] for x in transformed)
    print("Aggregated total:", total)


if __name__ == "__main__":
    start = time.perf_counter()
    asyncio.run(main())
    print("Elapsed:", time.perf_counter() - start)
