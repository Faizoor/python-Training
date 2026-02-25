import time
import random
import functools

def retry_backoff(max_attempts=5, base_delay=0.05, jitter=0.02, retry_on=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on as e:
                    last_exc = e
                    if attempt == max_attempts:
                        print(f"{func.__name__}: exhausted after {attempt}")
                        raise
                    delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, jitter)
                    print(f"{func.__name__}: retry {attempt} after {delay:.3f}s")
                    time.sleep(delay)
            raise last_exc
        return wrapper
    return decorator


@retry_backoff(max_attempts=3, base_delay=0.02)
def fragile_call(should_fail=True):
    if should_fail and random.random() < 0.7:
        raise RuntimeError('flaky')
    return 'ok'

if __name__ == '__main__':
    try:
        print(fragile_call())
    except Exception as e:
        print('fragile_call failed:', e)
