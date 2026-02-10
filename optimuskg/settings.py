import os
import warnings

# Ignore warnings about dataset names containing '.' characters.
# We use '.' in dataset names to indicate a hierarchy of datasets.
# NOTE: The warnings.filterwarnings call suppresses these in the main process.
# The PYTHONWARNINGS env var is needed for ParallelRunner child processes
# (spawn mode on macOS), where settings.py is never imported because
# _bootstrap_subprocess is skipped when PACKAGE_NAME is None.
_DOT_WARNING_FILTER = "ignore::UserWarning:kedro.pipeline.node"
_existing_py_warnings = os.environ.get("PYTHONWARNINGS", "")
if _DOT_WARNING_FILTER not in _existing_py_warnings:
    os.environ["PYTHONWARNINGS"] = (
        f"{_existing_py_warnings},{_DOT_WARNING_FILTER}"
        if _existing_py_warnings
        else _DOT_WARNING_FILTER
    )

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
