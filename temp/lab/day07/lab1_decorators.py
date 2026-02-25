import time
import random
import functools

def _short_repr(arg, max_len=60):
    r = repr(arg)
    return r if len(r) <= max_len else r[:max_len-3] + '...'

def log_calls(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_summary = ', '.join([_short_repr(a) for a in args] +
                                 [f"{k}={_short_repr(v)}" for k, v in kwargs.items()])
        print(f"CALL {func.__name__}({args_summary})")
        return func(*args, **kwargs)
    return wrapper

def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = (time.perf_counter() - start) * 1000.0
            print(f"TIME {func.__name__}: {elapsed:.2f} ms")
    return wrapper

def retry(max_attempts=3, delay=0.05):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    print(f"{func.__name__}: attempt {attempt}")
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if attempt == max_attempts:
                        print(f"{func.__name__}: exhausted after {attempt} attempts")
                        raise
                    time.sleep(delay)
            raise last_exc
        return wrapper
    return decorator


@retry(max_attempts=4, delay=0.02)
@timeit
@log_calls
def fetch_data():
    """Simulated API fetch: randomly raises RuntimeError to simulate failure."""
    if random.random() < 0.4:
        raise RuntimeError("simulated fetch failure")
    time.sleep(0.02)
    return {"rows": [1, 2, 3]}


if __name__ == '__main__':
    try:
        print(fetch_data())
    except Exception as e:
        print("fetch_data failed:", e)
