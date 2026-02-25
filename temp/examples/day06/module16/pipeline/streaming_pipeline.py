from typing import Iterator, Dict, Any
import re


def read_file(filepath: str) -> Iterator[str]:
    with open(filepath, "r") as f:
        for line in f:
            yield line.strip()


def filter_errors(lines: Iterator[str]) -> Iterator[str]:
    for line in lines:
        if "ERROR" in line:
            yield line


LOG_PATTERN = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
    r" \| (?P<level>\w+)"
    r" \| (?P<message>.+)"
)


def transform_records(lines: Iterator[str]) -> Iterator[Dict[str, Any]]:
    for line in lines:
        m = LOG_PATTERN.match(line)
        if m:
            yield m.groupdict()
        else:
            print(f"[WARN] Skipping malformed line: {line[:60]}")


def count_records(records: Iterator[Dict[str, Any]]) -> int:
    count = 0
    for _ in records:
        count += 1
    return count


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python streaming_pipeline.py /path/to/logfile")
        sys.exit(1)

    logfile = sys.argv[1]
    lines = read_file(logfile)
    errors = filter_errors(lines)
    records = transform_records(errors)
    total = count_records(records)
    print(f"Total ERROR records processed: {total}")
