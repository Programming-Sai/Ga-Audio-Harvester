# input_layer/loader.py
import json
from pathlib import Path
from .defaults import DEFAULT_CONFIG

def deep_merge(base: dict, override: dict) -> dict:
    for k, v in override.items():
        if isinstance(v, dict) and k in base:
            base[k] = deep_merge(base.get(k, {}), v)
        else:
            if v is not None:
                base[k] = v
    return base

def load_config(config_path="config.json", cli_config=None):
    config = DEFAULT_CONFIG.copy()

    path = Path(config_path)
    if path.exists():
        with open(path, "r") as f:
            file_cfg = json.load(f)
            config = deep_merge(config, file_cfg)

    if cli_config:
        config = deep_merge(config, cli_config)

    return config


def load_queries(path_to_query_file):
    with open(path_to_query_file, "r") as q:
        queries = [x.strip() for x in q.readlines() if x.strip()]
    
    return queries
