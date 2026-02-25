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
    path = 'data/large.txt'
    # Chain generators
    gen = transform_line(filter_keyword(read_file(path), 'ERROR'))

    # Print first 10 results
    for item in islice(gen, 10):
        print(item)

    # Convert remaining generator to list
    remaining = list(gen)
    print(f"Remaining items after taking first 10: {len(remaining)}")
