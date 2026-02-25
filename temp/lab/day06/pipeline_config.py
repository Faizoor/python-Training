import os
from pathlib import Path
import yaml


def load_config():
    env = os.environ.get('ENV', 'dev')
    # Resolve config path relative to this file so cwd doesn't matter
    base = Path(__file__).resolve().parent
    path = base / 'config' / f'{env}.yaml'
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
