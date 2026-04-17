"""Public loader API: fetch files, DataFrames, or a NetworkX graph."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any, Final

import networkx as nx
import polars as pl

from optimuskg import _dataverse, _graph

_FULL: Final[tuple[str, str]] = ("nodes.parquet", "edges.parquet")
_LCC: Final[tuple[str, str]] = ("largest_connected_component_nodes.parquet", "largest_connected_component_edges.parquet")


def get_file(relative_path: str, *, force: bool = False) -> Path:
    """Return the local path to a parquet file, downloading from Dataverse if missing.

    Paths mirror the layout under ``data/gold/kg/parquet/`` in the source repo.
    """
    return _dataverse.download(relative_path, force=force)


def load_parquet(
    relative_path: str,
    *,
    force: bool = False,
    **read_parquet_kwargs: Any,
) -> pl.DataFrame:
    """Download (if needed) and read a parquet file as a Polars DataFrame."""
    path = get_file(relative_path, force=force)
    return pl.read_parquet(path, **read_parquet_kwargs)


def load_graph(
    *,
    lcc: bool = False,
    force: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return ``(nodes, edges)`` Polars DataFrames for the full graph or its LCC."""
    nodes_file, edges_file = _LCC if lcc else _FULL
    nodes = load_parquet(nodes_file, force=force)
    edges = load_parquet(edges_file, force=force)
    return nodes, edges


def load_networkx(
    *,
    lcc: bool = False,
    force: bool = False,
    parse_properties: bool = True,
) -> nx.MultiDiGraph:
    """Return the graph as a ``nx.MultiDiGraph`` with properties loaded onto attrs."""
    if not lcc:
        warnings.warn(
            "Loading the full graph into NetworkX requires several GB of memory "
            "(190k nodes, 21M edges). Pass lcc=True for a slightly smaller, "
            "connected variant.",
            stacklevel=2,
        )
    nodes, edges = load_graph(lcc=lcc, force=force)
    return _graph.build_multidigraph(nodes, edges, parse_properties=parse_properties)
