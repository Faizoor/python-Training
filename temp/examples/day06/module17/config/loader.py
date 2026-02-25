import yaml
import os
from typing import Any, Dict

CONFIG_DIR = os.path.dirname(__file__)


def load_config() -> Dict[str, Any]:
    env = os.environ.get("ENV", "dev").lower()
    config_file = os.path.join(CONFIG_DIR, f"{env}.yaml")
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"No config file found for environment '{env}': {config_file}")
    print(f"[ConfigLoader] Loading config for environment: {env}")
    with open(config_file, "r") as f:
        return yaml.safe_load(f)
