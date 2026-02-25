def read_log_lines(filepath: str):
    with open(filepath, "r") as f:
        for line in f:
            yield line.strip()
