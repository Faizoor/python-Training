"""
pipeline/exceptions.py
Custom exception hierarchy for the capstone pipeline.
Module 20 – Custom exceptions in pipelines.
"""


class PipelineError(Exception):
    """Base class for all pipeline errors."""

    def __init__(self, message: str, context: dict | None = None):
        super().__init__(message)
        self.context = context or {}

    def __str__(self):
        base = super().__str__()
        if self.context:
            ctx = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{base} [{ctx}]"
        return base


# ── Data-layer errors ──────────────────────────────────────────────────────────

class DataReadError(PipelineError):
    """Raised when a data source cannot be read."""


class DataWriteError(PipelineError):
    """Raised when a data sink cannot be written."""


# ── Validation errors ──────────────────────────────────────────────────────────

class ValidationError(PipelineError):
    """Raised when a record fails validation."""

    def __init__(self, message: str, field: str = "", value=None, **kwargs):
        super().__init__(message, context={"field": field, "value": value, **kwargs})
        self.field = field
        self.value = value


class SchemaError(PipelineError):
    """Raised when the source schema doesn't match expectations."""


# ── Transformation errors ──────────────────────────────────────────────────────

class TransformError(PipelineError):
    """Raised when a transformation step fails."""


# ── Config errors ──────────────────────────────────────────────────────────────

class ConfigError(PipelineError):
    """Raised for bad or missing configuration."""


# ── Network / external service errors ─────────────────────────────────────────

class APIError(PipelineError):
    """Raised when an external API call fails."""

    def __init__(self, message: str, status_code: int = 0, **kwargs):
        super().__init__(message, context={"status_code": status_code, **kwargs})
        self.status_code = status_code


class RetryExhaustedError(PipelineError):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, message: str, attempts: int = 0, last_error: Exception | None = None):
        super().__init__(message, context={"attempts": attempts})
        self.attempts = attempts
        self.last_error = last_error
