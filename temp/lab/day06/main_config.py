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
