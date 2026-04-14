"""Client for the OptimusKG biomedical knowledge graph on Harvard Dataverse."""

from optimuskg._config import (
    get_cache_dir,
    get_doi,
    get_server,
    set_cache_dir,
    set_doi,
    set_server,
)
from optimuskg.api import get_file, load_graph, load_networkx, load_parquet

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "get_cache_dir",
    "get_doi",
    "get_file",
    "get_server",
    "load_graph",
    "load_networkx",
    "load_parquet",
    "set_cache_dir",
    "set_doi",
    "set_server",
]
