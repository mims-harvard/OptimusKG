import warnings

import polars as pl

warnings.filterwarnings(
    "ignore",
    message="Dataset name '.*' contains '.' characters.*",
    category=UserWarning,
)

from optimuskg.hooks import ChecksumHooks, OriginHooks, QualityChecksHooks

# Hooks are executed in a Last-In-First-Out (LIFO) order.
HOOKS = (QualityChecksHooks(), ChecksumHooks(), OriginHooks())

# Installed plugins for which to disable hook auto-registration.
# DISABLE_HOOKS_FOR_PLUGINS = ("kedro-viz",)

# Class that manages storing KedroSession data.
# from kedro.framework.session.store import BaseSessionStore
# SESSION_STORE_CLASS = BaseSessionStore
# Keyword arguments to pass to the `SESSION_STORE_CLASS` constructor.
# SESSION_STORE_ARGS = {
#     "path": "./sessions"
# }

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


# Class that manages Kedro's library components.
# from kedro.framework.context import KedroContext
# CONTEXT_CLASS = KedroContext
