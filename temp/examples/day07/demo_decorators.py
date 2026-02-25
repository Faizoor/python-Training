import logging
import time
import random
from functools import wraps

logger = logging.getLogger("demo.decorators")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def logging_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info("Calling %s with args=%s kwargs=%s", func.__name__, args, kwargs)
        result = func(*args, **kwargs)
        logger.info("%s returned %s", func.__name__, type(result))
        return result

    return wrapper


def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - start
            logger.info("%s took %.3f s", func.__name__, elapsed)

    return wrapper


def retry_decorator(max_attempts=3, exceptions=(Exception,)):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    logger.debug("Attempt %s for %s", attempt, func.__name__)
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    logger.warning("%s failed on attempt %s: %s", func.__name__, attempt, exc)
                    if attempt == max_attempts:
                        logger.error("Max attempts reached for %s. Re-raising.", func.__name__)
                        raise
                    time.sleep(0.1 * attempt)

        return wrapper

    return decorator


@logging_decorator
@timing_decorator
@retry_decorator(max_attempts=4, exceptions=(RuntimeError,))
def simulated_api_fetch(resource_id: int) -> dict:
    """Simulated flaky API function used in an ETL ingestion stage.

    - Randomly fails to simulate network/API instability.
    - Sleeps to simulate IO latency.
    """
    # Simulate network latency
    time.sleep(random.uniform(0.05, 0.2))
    # Simulate flakiness
    if random.random() < 0.4:
        raise RuntimeError(f"Transient error fetching resource {resource_id}")
    return {"id": resource_id, "payload": f"data-for-{resource_id}"}


if __name__ == "__main__":
    for i in range(1, 6):
        try:
            data = simulated_api_fetch(i)
            print("Fetched:", data)
        except Exception as e:
            print("Final failure for id", i, "->", e)
