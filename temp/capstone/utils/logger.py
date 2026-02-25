"""
utils/logger.py
Centralised logging configuration.
Module 20 – Logging best practices: handlers, formatters, levels.
"""

from __future__ import annotations

import logging
import logging.config
import sys
from pathlib import Path


# ── Default log directory ──────────────────────────────────────────────────────
_DEFAULT_LOG_DIR = Path(__file__).parent.parent / "logs"


def setup_logging(
    level: str = "INFO",
    log_dir: Path | str | None = None,
    log_filename: str = "pipeline.log",
    enable_file_handler: bool = True,
) -> None:
    """
    Configure root logger with:
        - ColourConsoleHandler  → stdout  (INFO and above, human-readable)
        - RotatingFileHandler   → logfile (DEBUG and above, machine-readable)

    Module 20 best practices:
        • Single call-site configuration via logging.config.dictConfig.
        • Separate log levels for console (INFO) and file (DEBUG).
        • Structured format includes %(asctime)s, %(name)s, %(levelname)s.
        • Using %(funcName)s to trace execution flow.
    """
    log_dir = Path(log_dir) if log_dir else _DEFAULT_LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / log_filename

    # Map string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    config: dict = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(asctime)s | %(name)-30s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "simple": {
                "format": "%(asctime)s | %(levelname)-8s | %(message)s",
                "datefmt": "%H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "level": max(numeric_level, logging.INFO),
                "formatter": "simple",
            },
        },
        "root": {
            "level": "DEBUG",
            "handlers": ["console"],
        },
        "loggers": {
            # Suppress chatty third-party loggers
            "urllib3":   {"level": "WARNING"},
            "asyncio":   {"level": "WARNING"},
            "aiohttp":   {"level": "WARNING"},
        },
    }

    if enable_file_handler:
        config["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": str(log_file),
            "maxBytes": 10 * 1024 * 1024,   # 10 MB
            "backupCount": 3,
            "encoding": "utf-8",
            "level": "DEBUG",
            "formatter": "detailed",
        }
        config["root"]["handlers"].append("file")

    logging.config.dictConfig(config)
    logging.getLogger(__name__).info(
        "🔧  Logging configured (console=%s, file=%s)",
        level,
        str(log_file) if enable_file_handler else "disabled",
    )


def get_logger(name: str) -> logging.Logger:
    """Convenience wrapper so modules can do: logger = get_logger(__name__)"""
    return logging.getLogger(name)
