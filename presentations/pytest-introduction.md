# Introduction to pytest

## Why pytest?

**pytest** is the standard testing framework for Python. It is:

- Simple to write — plain functions, no boilerplate classes required
- Descriptive failure messages out of the box
- Extensible via plugins (coverage, mock, async, etc.)

Install:

```bash
pip install pytest
```

---

## Writing Your First Test

A test is any function that starts with `test_`.

```python
# test_transform.py

def add(a, b):
    return a + b

def test_add():
    assert add(2, 3) == 5

def test_add_negative():
    assert add(-1, 1) == 0
```

Run all tests:

```bash
pytest
```

Run a specific file:

```bash
pytest test_transform.py
```

Run a specific test:

```bash
pytest test_transform.py::test_add
```

---

## Assertions

pytest rewrites `assert` statements to show detailed diffs on failure — no need for special assertion methods.

```python
def test_output():
    result = {"rows": 10, "status": "ok"}
    assert result["rows"] == 10
    assert result["status"] == "ok"
```

On failure pytest prints the exact values that did not match.

---

## Testing for Exceptions

Use `pytest.raises` to assert that a specific exception is raised:

```python
import pytest

def divide(a, b):
    if b == 0:
        raise ValueError("Cannot divide by zero")
    return a / b

def test_divide_by_zero():
    with pytest.raises(ValueError, match="Cannot divide by zero"):
        divide(10, 0)
```

---

## Fixtures

A **fixture** is a reusable piece of setup (and optional teardown) that is injected into tests by name.

### Defining a fixture

```python
import pytest

@pytest.fixture
def sample_data():
    return [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob",   "age": 25},
    ]
```

### Using a fixture

Just add the fixture name as a parameter — pytest injects it automatically:

```python
def test_record_count(sample_data):
    assert len(sample_data) == 2

def test_first_record(sample_data):
    assert sample_data[0]["name"] == "Alice"
```

### Fixture with teardown (`yield`)

```python
@pytest.fixture
def temp_file(tmp_path):
    path = tmp_path / "test.csv"
    path.write_text("id,name\n1,Alice\n")
    yield path                  # test runs here
    path.unlink(missing_ok=True)  # cleanup after test
```

---

## Built-in Fixtures

pytest ships with useful built-in fixtures:

| Fixture | What it provides |
|---|---|
| `tmp_path` | A temporary directory unique to the test |
| `capsys` | Capture stdout / stderr output |
| `monkeypatch` | Patch objects, environment variables, etc. |
| `caplog` | Capture log records |

```python
def test_log_output(caplog):
    import logging
    logging.warning("pipeline started")
    assert "pipeline started" in caplog.text
```

---

## Parametrize — Test Multiple Inputs

Run the same test with different inputs using `@pytest.mark.parametrize`:

```python
import pytest

@pytest.mark.parametrize("value, expected", [
    ("42",   42),
    ("-5",  -5),
    ("0",    0),
])
def test_parse_int(value, expected):
    assert int(value) == expected
```

pytest will run three separate test cases and report each individually.

---

## Organising Tests

```
project/
├── pipeline/
│   └── transformers.py
└── tests/
    ├── __init__.py
    ├── conftest.py       ← shared fixtures live here
    └── test_transformers.py
```

`conftest.py` is automatically loaded by pytest — put shared fixtures there so all test files can use them without importing.

---

## Practical Example (Data Engineering)

```python
# tests/conftest.py
import pytest

@pytest.fixture
def raw_records():
    return [
        {"id": "1", "amount": "100.5", "status": "active"},
        {"id": "2", "amount": "200.0", "status": "inactive"},
    ]
```

```python
# tests/test_transformers.py
def clean(record):
    return {
        "id": int(record["id"]),
        "amount": float(record["amount"]),
        "active": record["status"] == "active",
    }

def test_clean_converts_types(raw_records):
    result = clean(raw_records[0])
    assert isinstance(result["id"], int)
    assert isinstance(result["amount"], float)
    assert result["active"] is True

def test_clean_inactive(raw_records):
    result = clean(raw_records[1])
    assert result["active"] is False
```

---

## Useful CLI Options

| Command | Purpose |
|---|---|
| `pytest -v` | Verbose — show each test name |
| `pytest -k "transform"` | Run tests whose name contains "transform" |
| `pytest -x` | Stop after first failure |
| `pytest --tb=short` | Shorter traceback output |
| `pytest --co` | Collect (list) tests without running them |

---

## Key Takeaways

- Tests are plain functions prefixed with `test_`.
- Use `assert` directly — pytest gives clear failure messages.
- **Fixtures** keep setup DRY and reusable.
- Put shared fixtures in `conftest.py`.
- **Parametrize** to test multiple cases without duplicate code.
- `pytest.raises` tests that the right exception is thrown.
