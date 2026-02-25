import asyncio
from concurrent.futures import ProcessPoolExecutor
from lab5_asyncio import fetch_sim

def transform(payload):
    s = 0
    for i in range(5_000 + payload['id']):
        s += (i % 7)
    return {**payload, 'transformed': s}

async def fetch_and_transform(id, executor):
    payload = await fetch_sim(id, delay=0.02)
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(executor, transform, payload)
    return result

async def run_pipeline(n):
    executor = ProcessPoolExecutor(max_workers=2)
    tasks = [fetch_and_transform(i, executor) for i in range(n)]
    results = await asyncio.gather(*tasks)
    executor.shutdown()
    return results

if __name__ == '__main__':
    res = asyncio.run(run_pipeline(6))
    print(res[:2])
