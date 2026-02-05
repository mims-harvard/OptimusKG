import warnings

# Ignore warnings about dataset names containing '.' characters.
# We use '.' in dataset names to indicate a hierarchy of datasets.
# NOTE: This must be set before importing hooks, which trigger node registration.
warnings.filterwarnings(
    "ignore",
    message="Dataset name '.*' contains '.' characters.*",
    category=UserWarning,
)

from optimuskg.hooks import ChecksumHooks, OriginHooks, QualityChecksHooks  # noqa: E402
from optimuskg.utils import parse_polars_type  # noqa: E402

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
        "pl": parse_polars_type,
    },
    "merge_strategy": {
        "base": "soft",
        "local": "soft",
    },
}
