import random
import functools
import time

class Retry:
    def __init__(self, max_attempts=3, delay=0.02):
        self.max_attempts = max_attempts
        self.delay = delay

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, self.max_attempts + 1):
                print(f"{func.__name__}: attempt {attempt}")
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == self.max_attempts:
                        print(f"{func.__name__}: final failure")
                        raise
                    time.sleep(self.delay)
        return wrapper


@Retry(max_attempts=3)
def load_data():
    if random.random() < 0.5:
        raise ValueError("transient load error")
    return "ok"


if __name__ == '__main__':
    try:
        print(load_data())
    except Exception as e:
        print("load_data failed:", e)
