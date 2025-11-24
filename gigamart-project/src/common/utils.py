from typing import Optional, Dict, Any
import os
import yaml


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load GigaMart pipeline configuration from a YAML file.

    """
    config_path = path or os.environ.get("GIGAMART_CONFIG_PATH", "config/config_dev.yaml")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
