import asyncio
import time

async def fetch_sim(id, delay=0.05):
    await asyncio.sleep(delay)
    return {'id': id, 'data': id * 2}

async def run_sequential(n):
    results = []
    for i in range(n):
        results.append(await fetch_sim(i))
    return results

async def run_concurrent(n):
    tasks = [fetch_sim(i) for i in range(n)]
    return await asyncio.gather(*tasks)

if __name__ == '__main__':
    n = 8
    t0 = time.perf_counter()
    res = asyncio.run(run_sequential(n))
    t_seq = time.perf_counter() - t0
    t0 = time.perf_counter()
    res2 = asyncio.run(run_concurrent(n))
    t_conc = time.perf_counter() - t0
    print(f"sequential {t_seq:.3f}s, concurrent {t_conc:.3f}s")
