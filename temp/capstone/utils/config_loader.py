"""
utils/config_loader.py
YAML-based configuration loader with environment resolution.
Module 17 – Config-driven & modular design, dev/stage/prod environments.
"""

from __future__ import annotations

import os
import copy
import logging
from pathlib import Path

import yaml  # PyYAML

from pipeline.exceptions import ConfigError

logger = logging.getLogger(__name__)

# Directory that holds dev.yaml / prod.yaml (relative to this file's grandparent)
_DEFAULT_CONFIG_DIR = Path(__file__).parent.parent / "config"


def load_config(env: str | None = None, config_dir: Path | str | None = None) -> dict:
    """
    Load configuration for the given environment.

    Resolution order:
        1. ``env`` argument
        2. ``PIPELINE_ENV`` environment variable
        3. Falls back to ``dev``

    Args:
        env:        "dev" | "prod" (or any filename stem in config_dir).
        config_dir: Directory that contains <env>.yaml files.

    Returns:
        Merged config dict (base values overridden by env-specific values).

    Raises:
        ConfigError: If the file is missing or YAML is invalid.
    """
    resolved_env = env or os.getenv("PIPELINE_ENV", "dev")
    cfg_dir = Path(config_dir) if config_dir else _DEFAULT_CONFIG_DIR

    env_file = cfg_dir / f"{resolved_env}.yaml"
    if not env_file.exists():
        raise ConfigError(
            f"Config file not found: {env_file}",
            context={"env": resolved_env, "config_dir": str(cfg_dir)},
        )

    logger.info("⚙  Loading config: %s (env=%s)", env_file, resolved_env)
    try:
        with open(env_file) as f:
            config = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {env_file}: {exc}") from exc

    config["_env"] = resolved_env
    config.setdefault("pipeline", {})["env"] = resolved_env

    logger.debug("Config loaded: %s", config)
    return config


def validate_config(config: dict) -> None:
    """
    Validate that required top-level keys are present.
    Raises ConfigError listing every missing key.
    """
    required = ["pipeline", "source", "output"]
    missing = [k for k in required if k not in config]
    if missing:
        raise ConfigError(
            f"Config missing required keys: {missing}",
            context={"present": list(config.keys())},
        )

    # Validate source section
    src = config.get("source", {})
    if "type" not in src:
        raise ConfigError("config.source.type is required")

    # Validate output section
    out = config.get("output", {})
    if "dir" not in out:
        raise ConfigError("config.output.dir is required")

    logger.info("✔  Config validation passed")


def merge_configs(*configs: dict) -> dict:
    """
    Deep-merge an arbitrary number of config dicts (later dicts win).
    Useful for combining base config with CLI overrides.
    """
    result: dict = {}
    for cfg in configs:
        _deep_merge(result, cfg)
    return result


def _deep_merge(base: dict, override: dict) -> dict:
    for key, val in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(val, dict):
            _deep_merge(base[key], val)
        else:
            base[key] = copy.deepcopy(val)
    return base
