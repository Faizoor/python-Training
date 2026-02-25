from typing import Any, Dict, List


class ConfigValidationError(Exception):
    pass


REQUIRED_KEYS = [
    ("database", "host"),
    ("database", "port"),
    ("database", "name"),
    ("paths", "input"),
    ("paths", "output"),
    ("pipeline", "batch_size"),
    ("pipeline", "log_level"),
]


def validate_config(config: Dict[str, Any]) -> None:
    missing: List[str] = []
    for section, key in REQUIRED_KEYS:
        if section not in config:
            missing.append(f"[{section}]")
        elif key not in config[section]:
            missing.append(f"[{section}].{key}")

    if missing:
        raise ConfigValidationError("Configuration is missing required keys:\n" + "\n".join(f"  - {m}" for m in missing))

    batch_size = config["pipeline"]["batch_size"]
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ConfigValidationError(f"[pipeline].batch_size must be a positive integer. Got: {batch_size!r}")

    print("[ConfigValidator] Configuration is valid.")
