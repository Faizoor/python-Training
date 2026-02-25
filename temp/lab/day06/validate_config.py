import os
from pathlib import Path
import yaml

REQUIRED_KEYS = {'input_path', 'keyword'}


def load_and_validate(env: str = None):
    if env is None:
        env = os.environ.get('ENV', 'dev')
    base = Path(__file__).resolve().parent
    path = base / 'config' / f'{env}.yaml'
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
    base = Path(__file__).resolve().parent
    bad_path = base / 'config' / 'bad.yaml'
    with open(bad_path, 'w', encoding='utf-8') as f:
        f.write('input_path: lab/data/large.txt\n')
    try:
        cfg = load_and_validate('bad')
    except Exception as e:
        print('Expected failure for bad config:', e)
