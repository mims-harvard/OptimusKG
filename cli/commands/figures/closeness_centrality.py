"""Approximate closeness centrality distribution.

Plots a histogram of approximate closeness centrality values for every node
in the knowledge graph.  Closeness is approximated by running BFS from a
random sample of source nodes, which keeps the computation tractable for
large graphs.
"""

from __future__ import annotations

import logging
import random
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import polars as pl

from cli.commands.metrics.utils import load_parquet_dir

from . import style  # noqa: F401
from .style import apply_axis_styling

logger = logging.getLogger(__name__)

# Number of random BFS sources used to approximate closeness centrality.
_N_SAMPLES = 100
_RANDOM_SEED = 42


def _build_graph(edges_dir: Path) -> nx.Graph:
    """Build an undirected NetworkX graph from all edge parquet files."""

    G = nx.Graph()
    for df in load_parquet_dir(edges_dir):
        edge_pairs = df.select("from", "to").iter_rows()
        G.add_edges_from(edge_pairs)
    logger.info(
        "Built graph with %s nodes and %s edges",
        G.number_of_nodes(),
        G.number_of_edges(),
    )
    return G


def compute_data(nodes_dir: Path, edges_dir: Path) -> pl.DataFrame:
    """Compute approximate closeness centrality for every node.

    Uses BFS from *_N_SAMPLES* randomly chosen source nodes to estimate
    the average shortest-path distance to each target node.  Closeness is
    the reciprocal of this average distance.

    Returns a ``pl.DataFrame`` with a single column:
      - ``closeness`` (Float64) – approximate closeness centrality
    """

    G = _build_graph(edges_dir)
    nodes = list(G.nodes())
    n_nodes = len(nodes)
    logger.info("Graph has %s nodes and %s edges", n_nodes, G.number_of_edges())

    rng = random.Random(_RANDOM_SEED)
    sources = rng.sample(nodes, min(_N_SAMPLES, n_nodes))
    logger.info("Running BFS from %s random sources ...", len(sources))

    # Accumulate sum-of-distances and count-of-reachable-sources per node.
    dist_sum: dict[str, int] = {}
    dist_count: dict[str, int] = {}

    for i, src in enumerate(sources, 1):
        if i % 20 == 0 or i == len(sources):
            logger.info("  BFS %d / %d", i, len(sources))
        lengths = nx.single_source_shortest_path_length(G, src)
        for target, d in lengths.items():
            if d == 0:
                continue  # skip self
            dist_sum[target] = dist_sum.get(target, 0) + d
            dist_count[target] = dist_count.get(target, 0) + 1

    # Closeness = n_reached / sum_of_distances  (Wasserman-Faust variant).
    closeness_values: list[float] = []
    for node in nodes:
        s = dist_sum.get(node, 0)
        c = dist_count.get(node, 0)
        if s > 0:
            closeness_values.append(c / s)
        else:
            closeness_values.append(0.0)

    return pl.DataFrame({"closeness": closeness_values}).cast({"closeness": pl.Float64})


_BAR_COLOR = "#999999"


def render_plot(data: pl.DataFrame, out_path: Path) -> None:
    """Render the closeness centrality histogram and save as PDF."""

    values = data["closeness"].to_numpy().astype(float)

    # Drop zeros (disconnected / unreached nodes) before plotting.
    values = values[values > 0]

    fig, ax = plt.subplots(figsize=(4, 3.5))

    ax.hist(values, bins=80, color=_BAR_COLOR, edgecolor="none")

    ax.set_yscale("log")

    ax.set_xlabel("Closeness centrality", fontsize=8, fontweight="bold")
    ax.set_ylabel("Count", fontsize=8, fontweight="bold")
    ax.tick_params(axis="both", labelsize=7)

    apply_axis_styling(ax)

    plt.tight_layout(pad=0.4)
    plt.savefig(out_path)
    plt.close(fig)
