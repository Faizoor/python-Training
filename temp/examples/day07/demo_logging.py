import logging


def configure_logging():
    fmt = "%(asctime)s level=%(levelname)s name=%(name)s msg=\"%(message)s\""
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt))

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)


def run():
    configure_logging()
    logger = logging.getLogger("pipeline.ingest")
    logger.info("start run batch_id=%s", 42)
    try:
        logger.debug("debugging details that are verbose")
        raise ValueError("simulated error")
    except Exception:
        logger.exception("Error while processing batch")


if __name__ == "__main__":
    run()
