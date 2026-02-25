import time
import random
import logging
from functools import wraps

logger = logging.getLogger("demo.class_decorator")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class Retry:
    def __init__(self, max_attempts=3, exceptions=(Exception,), backoff=0.1):
        self.max_attempts = max_attempts
        self.exceptions = exceptions
        self.backoff = backoff
        self.stats = {"calls": 0, "retries": 0}

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            self.stats["calls"] += 1
            last_exc = None
            for attempt in range(1, self.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except self.exceptions as exc:
                    last_exc = exc
                    self.stats["retries"] += 1
                    logger.warning("Attempt %s failed for %s: %s", attempt, func.__name__, exc)
                    if attempt == self.max_attempts:
                        logger.error("Raising after %s attempts", attempt)
                        raise
                    time.sleep(self.backoff * attempt)

        return wrapper



@Retry(max_attempts=4, backoff=0.05)
def load_data_chunk(chunk_id: int) -> list:
    """Simulated data loader for an ETL stage."""
    if random.random() < 0.5:
        raise ConnectionError(f"Failed to load chunk {chunk_id}")
    return [{"chunk": chunk_id, "value": i} for i in range(3)]


if __name__ == "__main__":
    for i in range(1, 6):
        try:
            print("Loaded:", load_data_chunk(i))
        except Exception as e:
            print("Chunk failed permanently:", i, e)
