import polars as pl
from kedro.io import KedroDataCatalog

from optimuskg.hooks import ChecksumHooks, OriginHooks, QualityChecksHooks

# Hooks are executed in a Last-In-First-Out (LIFO) order.
HOOKS = (QualityChecksHooks(), ChecksumHooks(), OriginHooks())

# Directory that holds configuration.
CONF_SOURCE = "conf"

# Class that manages how configuration is loaded.
from kedro.config import OmegaConfigLoader  # noqa: E402

CONFIG_LOADER_CLASS = OmegaConfigLoader
CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
    "custom_resolvers": {
        "pl": lambda x: getattr(pl, x),  # Polars types
    },
    "merge_strategy": {
        "base": "soft",
        "local": "soft",
    },
}

# Class that manages the Data Catalog.
DATA_CATALOG_CLASS = KedroDataCatalog
