import time
from math import sqrt
from concurrent.futures import ProcessPoolExecutor

def is_prime(n):
    if n < 2:
        return False
    for i in range(2, int(sqrt(n)) + 1):
        if n % i == 0:
            return False
    return True

def heavy_task(x):
    count = 0
    for i in range(10_000 + x, 10_000 + x + 1000):
        if is_prime(i):
            count += 1
    return count

def run_sequential(inputs):
    start = time.perf_counter()
    results = [heavy_task(x) for x in inputs]
    elapsed = time.perf_counter() - start
    return results, elapsed

def run_parallel(inputs, max_workers=4):
    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        results = list(ex.map(heavy_task, inputs))
    elapsed = time.perf_counter() - start
    return results, elapsed


if __name__ == '__main__':
    inputs = list(range(0, 8))
    seq_res, seq_t = run_sequential(inputs)
    par_res, par_t = run_parallel(inputs)
    print(f"sequential {seq_t:.2f}s, parallel {par_t:.2f}s")
