import time
import random
from functools import wraps


class NonRetryableError(Exception):
    pass


def backoff_retry(max_attempts=5, base_delay=0.1, max_delay=2.0, jitter=0.1, retry_exceptions=(Exception,), non_retry_exceptions=()):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                attempt += 1
                try:
                    return func(*args, **kwargs)
                except non_retry_exceptions:
                    raise
                except retry_exceptions as exc:
                    if attempt >= max_attempts:
                        raise
                    backoff = min(max_delay, base_delay * (2 ** (attempt - 1)))
                    sleep_time = backoff * (1 + random.uniform(-jitter, jitter))
                    time.sleep(sleep_time)

        return wrapper

    return decorator


@backoff_retry(max_attempts=4, base_delay=0.05, jitter=0.2, retry_exceptions=(RuntimeError,), non_retry_exceptions=(NonRetryableError,))
def flaky_op(i):
    if i == 99:
        raise NonRetryableError("bad payload")
    if random.random() < 0.6:
        raise RuntimeError("transient failure")
    return f"ok:{i}"


if __name__ == "__main__":
    for i in range(5):
        try:
            print(flaky_op(i))
        except Exception as e:
            print("Failed permanently:", e)
