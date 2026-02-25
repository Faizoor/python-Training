# memory_problem_demo.py

import os
import sys

# Create a sample log file for demonstration
def create_sample_log(filepath: str, num_lines: int) -> None:
    with open(filepath, "w") as f:
        for i in range(num_lines):
            status = "ERROR" if i % 100 == 0 else "INFO"
            f.write(f"2024-01-15 10:{i%60:02d}:00 | {status} | Record processed: id={i}\n")


LOGFILE = "/tmp/demo_pipeline.log"
create_sample_log(LOGFILE, 50000000)

def read_line(filepath: str):
    with open(filepath, "r") as f:
        for line in f:
            yield line

# BAD APPROACH — loads all lines into memory at once
def count_errors_naive(filepath: str) -> int:
    errors = 0
    for line in read_line(filepath):
        if "ERROR" in line:
            errors += 1
    #errors = [l for l in lines if "ERROR" in l]
    return errors


# Measure memory impact
before = sys.getsizeof([])
result = count_errors_naive(LOGFILE)
print(f"Error count: {result}")
print(f"Note: all 500,000 lines were in RAM simultaneously.")
print(f"For a 10GB log file, this means 10GB+ of RAM usage before any processing occurs.")