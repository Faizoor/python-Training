# Exception Handling in Python

## What is an Exception?

An **exception** is an error that occurs during program execution. Instead of crashing silently, Python raises an exception object that you can catch and handle gracefully.

---

## Basic Exception Handling

### `try / except`

```python
try:
    result = 10 / 0
except ZeroDivisionError:
    print("Cannot divide by zero!")
```

### `else` — runs only if no exception was raised

```python
try:
    result = 10 / 2
except ZeroDivisionError:
    print("Division error!")
else:
    print(f"Result: {result}")   # Result: 5.0
```

### `finally` — always runs (cleanup code)

```python
try:
    f = open("data.csv")
    data = f.read()
except FileNotFoundError:
    print("File not found.")
finally:
    print("Done — with or without error.")
```

---

## Catching Multiple Exceptions

```python
try:
    value = int(input("Enter a number: "))
    result = 100 / value
except ValueError:
    print("That's not a number.")
except ZeroDivisionError:
    print("Cannot divide by zero.")
```

Use a tuple to catch several exceptions in one block:

```python
except (ValueError, TypeError) as e:
    print(f"Input error: {e}")
```

---

## The `as` Clause — Inspecting the Error

```python
try:
    open("missing_file.csv")
except FileNotFoundError as e:
    print(f"Error: {e}")
    # Error: [Errno 2] No such file or directory: 'missing_file.csv'
```

---

## Common Built-in Exceptions

| Exception | When it occurs |
|---|---|
| `ValueError` | Wrong value type (e.g. `int("abc")`) |
| `TypeError` | Wrong data type |
| `KeyError` | Missing key in a dict |
| `IndexError` | List index out of range |
| `FileNotFoundError` | File does not exist |
| `ZeroDivisionError` | Division by zero |
| `AttributeError` | Object has no such attribute |
| `ImportError` | Failed module import |

---

## Raising Exceptions

Use `raise` to trigger an exception intentionally:

```python
def load_file(path):
    if not path.endswith(".csv"):
        raise ValueError(f"Expected a CSV file, got: {path}")
    # ... load logic
```

Re-raise the current exception after logging it:

```python
except Exception as e:
    print(f"Logging error: {e}")
    raise   # re-raises the same exception
```

---

## Custom Exceptions

Define your own exception classes by inheriting from `Exception`.

### Simple custom exception

```python
class PipelineError(Exception):
    """Base exception for pipeline errors."""
    pass
```

### Custom exception with a message

```python
class DataValidationError(Exception):
    def __init__(self, field, message):
        self.field = field
        super().__init__(f"Validation failed on '{field}': {message}")
```

```python
raise DataValidationError("age", "Value must be a positive integer")
# DataValidationError: Validation failed on 'age': Value must be a positive integer
```

### Exception hierarchy (recommended pattern)

```python
class PipelineError(Exception):
    """Base class — catch all pipeline errors with this."""
    pass

class ExtractionError(PipelineError):
    """Raised when data extraction fails."""
    pass

class TransformationError(PipelineError):
    """Raised when a transformation step fails."""
    pass

class LoadError(PipelineError):
    """Raised when writing output fails."""
    pass
```

Usage:

```python
def extract(source):
    if not source:
        raise ExtractionError("Source path cannot be empty.")

try:
    extract("")
except PipelineError as e:          # catches any subclass
    print(f"Pipeline failed: {e}")
```

---

## Practical Pattern for Data Engineers

```python
import logging

logger = logging.getLogger(__name__)

class DataSourceError(Exception):
    pass

def read_csv(path):
    try:
        with open(path) as f:
            return f.read()
    except FileNotFoundError as e:
        logger.error("File missing: %s", path)
        raise DataSourceError(f"Could not read file: {path}") from e
```

> `raise ... from e` preserves the original traceback (exception chaining).

---

## Key Takeaways

- Catch **specific** exceptions, not bare `except:`.
- Use `finally` for cleanup (closing files, DB connections).
- Build a **hierarchy** of custom exceptions for your project.
- Use exception chaining (`raise X from e`) to keep the full context.
- Log errors before re-raising or swallowing them.
