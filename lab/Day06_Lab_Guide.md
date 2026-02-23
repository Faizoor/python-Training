# Day 06 – Lab Guide

## Lab 1 – Abstract Base Class and Inheritance

### Objective
Create and use an abstract base class to read data from multiple sources.

### Requirements
Step-by-step tasks:
1. Create a base abstract class `DataSource` using `abc`.
2. Define an abstract method `read()`.
3. Create `CSVReader` subclass implementing `read()`.
4. Create `MockDBReader` subclass implementing `read()`.
5. Create a function `run_pipeline(source)` that accepts `DataSource` and calls `read()`.
6. Execute pipeline with both subclasses.

### Starter Structure (Folder Layout)

- lab/
  - data/
    - sample.csv
  - datasource.py
  - readers.py
  - main.py

### Implementation

#### datasource.py
```python
from abc import ABC, abstractmethod

class DataSource(ABC):
    @abstractmethod
    def read(self):
        """Return an iterable of records (list of dicts or similar)."""
        raise NotImplementedError()
```

#### readers.py
```python
from datasource import DataSource
import csv

class CSVReader(DataSource):
    def __init__(self, path):
        self.path = path

    def read(self):
        with open(self.path, newline='', encoding='utf-8') as f:
            return list(csv.DictReader(f))

class MockDBReader(DataSource):
    def read(self):
        return [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
```

#### main.py
```python
from readers import CSVReader, MockDBReader
from datasource import DataSource


def run_pipeline(source: DataSource):
    data = source.read()
    for row in data:
        print(row)


if __name__ == '__main__':
    # Ensure lab/data/sample.csv exists with header id,name and a couple rows
    csv_reader = CSVReader('lab/data/sample.csv')
    mock_reader = MockDBReader()

    print("CSVReader output:")
    run_pipeline(csv_reader)

    print("\nMockDBReader output:")
    run_pipeline(mock_reader)
```

### Expected Output

Sample console output (assuming `lab/data/sample.csv` contains two rows):

```
CSVReader output:
{'id': '1', 'name': 'Alice'}
{'id': '2', 'name': 'Bob'}

MockDBReader output:
{'id': 1, 'name': 'Alice'}
{'id': 2, 'name': 'Bob'}
```

---

## Lab 2 – Method Overriding and super()

### Objective
Demonstrate method overriding and using `super()` to extend base behaviour.

### Requirements
1. Add `connect()` method in base class.
2. Override `connect()` in subclass.
3. Use `super()` properly.
4. Print connection steps clearly.

### Complete Implementation

Create `method_override.py`:

```python
class BaseConnection:
    def connect(self):
        print("Base: initializing connection")

class SubConnection(BaseConnection):
    def connect(self):
        print("Sub: pre-connection steps")
        super().connect()
        print("Sub: post-connection steps")


def main():
    conn = SubConnection()
    conn.connect()


if __name__ == '__main__':
    main()
```

Expected console output when run:

```
Sub: pre-connection steps
Base: initializing connection
Sub: post-connection steps
```

---

## Lab 3 – Entity Modeling

### Objective
Create a validated entity class and a transformation function to convert dictionaries into objects.

### Requirements
1. Create `User` entity class.
2. Add validation inside constructor.
3. Raise `ValueError` for invalid data.
4. Create a function to transform a dictionary into a `User` object.
5. Handle invalid records gracefully.

### Complete Implementation

Create `user.py`:

```python
import re

class User:
    def __init__(self, username: str, email: str, age: int):
        if not isinstance(username, str) or not username:
            raise ValueError('username must be a non-empty string')
        if not isinstance(email, str) or not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            raise ValueError('invalid email')
        if not isinstance(age, int) or age < 0:
            raise ValueError('age must be a non-negative integer')
        self.username = username
        self.email = email
        self.age = age

    def __repr__(self):
        return f"User(username={self.username!r}, email={self.email!r}, age={self.age!r})"
```

Create `transform_users.py`:

```python
from user import User


def dict_to_user(d: dict):
    try:
        return User(d['username'], d['email'], d['age'])
    except Exception as e:
        return None


if __name__ == '__main__':
    records = [
        {'username': 'alice', 'email': 'alice@example.com', 'age': 30},
        {'username': '', 'email': 'bad', 'age': -1},
        {'username': 'bob', 'email': 'bob@example.com', 'age': 25}
    ]

    users = []
    for r in records:
        u = dict_to_user(r)
        if u is None:
            print(f"Skipping invalid record: {r}")
        else:
            users.append(u)

    print('Valid users:')
    for u in users:
        print(u)
```

Expected console output:

```
Skipping invalid record: {'username': '', 'email': 'bad', 'age': -1}
Valid users:
User(username='alice', email='alice@example.com', age=30)
User(username='bob', email='bob@example.com', age=25)
```

---

## Lab 4 – Iterators and Generators

### Objective
Compare eager file reading with generator-based lazy reading and demonstrate generator exhaustion.

### Requirements
1. Create a large text file with sample data.
2. Implement `read_file()` using `readlines()`.
3. Implement `read_file_lazy()` using `yield`.
4. Implement `filter_errors()`.
5. Count matching records.
6. Demonstrate generator exhaustion.

### Complete Implementation

Create `gen_file_demo.py`:

```python
import os
from itertools import islice

DATA_PATH = 'lab/data/large.txt'


def create_sample_file(path: str, lines: int = 1000):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        for i in range(1, lines + 1):
            level = 'ERROR' if i % 50 == 0 else 'INFO'
            f.write(f"{level}: This is log line {i}\n")


def read_file(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        return f.readlines()


def read_file_lazy(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            yield line


def filter_errors(lines):
    for line in lines:
        if 'ERROR' in line:
            yield line


if __name__ == '__main__':
    create_sample_file(DATA_PATH, lines=500)

    # Eager read
    all_lines = read_file(DATA_PATH)
    errors = list(filter_errors(all_lines))
    print(f"Eager read - total lines: {len(all_lines)}, errors: {len(errors)}")

    # Lazy read
    lazy_lines = read_file_lazy(DATA_PATH)
    errors_gen = filter_errors(lazy_lines)

    # Count errors by iterating
    count = 0
    for _ in errors_gen:
        count += 1
    print(f"Lazy read - errors counted by iteration: {count}")

    # Demonstrate exhaustion
    # errors_gen is exhausted; iterating again yields zero
    remaining = list(errors_gen)
    print(f"Remaining after exhaustion: {len(remaining)}")
```

Expected console output sample:

```
Eager read - total lines: 500, errors: 10
Lazy read - errors counted by iteration: 10
Remaining after exhaustion: 0
```

---

## Lab 5 – Generator Pipeline Chaining

### Objective
Build a generator pipeline of readers, filters, and transformers; observe lazy evaluation and conversion behavior.

### Requirements
1. Create generator functions:
   - `read_file()`
   - `filter_keyword()`
   - `transform_line()`
2. Chain them together.
3. Print first 10 results only.
4. Convert generator to list once and observe behavior.

### Complete Implementation

Create `pipeline_chain.py`:

```python
from itertools import islice


def read_file(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            yield line.rstrip('\n')


def filter_keyword(lines, keyword: str):
    for line in lines:
        if keyword in line:
            yield line


def transform_line(lines):
    for line in lines:
        # simple transform: split level and message
        if ': ' in line:
            level, msg = line.split(': ', 1)
            yield {'level': level, 'message': msg}


if __name__ == '__main__':
    path = 'lab/data/large.txt'
    # Chain generators
    gen = transform_line(filter_keyword(read_file(path), 'ERROR'))

    # Print first 10 results
    for item in islice(gen, 10):
        print(item)

    # Convert remaining generator to list
    remaining = list(gen)
    print(f"Remaining items after taking first 10: {len(remaining)}")
```

Expected console output: first 10 dictionaries followed by a count of remaining items.

---

## Lab 6 – Config-Driven Design

### Objective
Use YAML configs to drive pipeline input and filter settings and select configuration via an environment variable.

### Requirements
1. Create `config/dev.yaml` and `config/prod.yaml`.
2. Add pipeline settings (`input_path`, `keyword`).
3. Load config using `yaml.safe_load`.
4. Use `ENV` environment variable to select config.
5. Refactor generator pipeline to use config values.

### Folder Structure

- lab/
  - config/
    - dev.yaml
    - prod.yaml
  - data/
    - large.txt
  - pipeline_config.py
  - main_config.py

### Complete Implementation

#### config/dev.yaml
```yaml
input_path: lab/data/large.txt
keyword: ERROR
```

#### config/prod.yaml
```yaml
input_path: lab/data/large.txt
keyword: INFO
```

#### pipeline_config.py
```python
import os
import yaml
from itertools import islice


def load_config():
    env = os.environ.get('ENV', 'dev')
    path = f'lab/config/{env}.yaml'
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def read_file(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            yield line.rstrip('\n')


def filter_keyword(lines, keyword: str):
    for line in lines:
        if keyword in line:
            yield line


def transform_line(lines):
    for line in lines:
        if ': ' in line:
            level, msg = line.split(': ', 1)
            yield {'level': level, 'message': msg}
```

#### main_config.py
```python
from pipeline_config import load_config, read_file, filter_keyword, transform_line
from itertools import islice


def main():
    config = load_config()
    path = config['input_path']
    keyword = config['keyword']

    gen = transform_line(filter_keyword(read_file(path), keyword))

    # Print first 10 results
    for item in islice(gen, 10):
        print(item)


if __name__ == '__main__':
    main()
```

### Execution Steps

Install dependency and run with different `ENV` values:

```bash
pip install pyyaml
# Run with dev config
ENV=dev python lab/main_config.py
# Run with prod config
ENV=prod python lab/main_config.py
```

---

## Lab 7 – Basic Config Validation

### Objective
Validate configuration and raise clear errors when required keys are missing.

### Requirements
1. Add validation to ensure required config keys exist.
2. Raise meaningful error if missing.
3. Demonstrate failure case.

### Complete Implementation

Create `validate_config.py`:

```python
import os
import yaml

REQUIRED_KEYS = {'input_path', 'keyword'}


def load_and_validate(env: str = None):
    if env is None:
        env = os.environ.get('ENV', 'dev')
    path = f'lab/config/{env}.yaml'
    with open(path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) or {}

    missing = REQUIRED_KEYS - cfg.keys()
    if missing:
        raise KeyError(f"Missing required config keys: {', '.join(sorted(missing))} in {path}")
    return cfg


if __name__ == '__main__':
    # Success case
    try:
        cfg = load_and_validate('dev')
        print('Dev config loaded and validated:', cfg)
    except Exception as e:
        print('Dev config validation failed:', e)

    # Failure case: create a temporary invalid config and demonstrate
    bad_path = 'lab/config/bad.yaml'
    with open(bad_path, 'w', encoding='utf-8') as f:
        f.write('input_path: lab/data/large.txt\n')
    try:
        cfg = load_and_validate('bad')
    except Exception as e:
        print('Expected failure for bad config:', e)
```

Expected console output sample:

```
Dev config loaded and validated: {'input_path': 'lab/data/large.txt', 'keyword': 'ERROR'}
Expected failure for bad config: "Missing required config keys: keyword in lab/config/bad.yaml"
```

---

# End of Lab Guide
