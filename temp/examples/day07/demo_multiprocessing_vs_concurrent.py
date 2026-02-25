import time
from concurrent.futures import ProcessPoolExecutor


def heavy_work(n: int) -> int:
    # CPU-bound: sum of prime checks up to a limit based on n
    count = 0
    limit = 20000 + (n % 5) * 1000
    for i in range(2, limit):
        is_prime = True
        for j in range(2, int(i**0.5) + 1):
            if i % j == 0:
                is_prime = False
                break
        if is_prime:
            count += 1
    return count


def run_sequential(tasks):
    start = time.perf_counter()
    results = [heavy_work(t) for t in tasks]
    return time.perf_counter() - start, results


def run_process_pool(tasks, workers=4):
    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(heavy_work, tasks))
    return time.perf_counter() - start, results


if __name__ == "__main__":
    tasks = list(range(32))
    t_seq, _ = run_sequential(tasks)
    print(f"Sequential time: {t_seq:.2f}s")

    t_proc, _ = run_process_pool(tasks, workers=4)
    print(f"ProcessPool time (4 workers): {t_proc:.2f}s")

    print("Note: CPU-bound tasks benefit from multiprocessing because the GIL is bypassed by separate processes.")
