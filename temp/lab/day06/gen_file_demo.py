import os
from itertools import islice

DATA_PATH = 'data/large.txt'


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
