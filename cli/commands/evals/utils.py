"""Shared utilities for evals commands."""

from __future__ import annotations

import logging
from pathlib import Path

import networkx as nx
import polars as pl

logger = logging.getLogger("cli")


def load_graph(edges_dir: Path) -> nx.Graph:
    """Build an undirected NetworkX graph from edge parquet files.

    Args:
        edges_dir: Directory containing edge parquet files.

    Returns:
        Undirected NetworkX graph with all edges.
    """
    G = nx.Graph()
    for path in sorted(edges_dir.glob("*.parquet")):
        df = pl.read_parquet(path)
        if df.height > 0:
            edge_pairs = df.select("from", "to").iter_rows()
            G.add_edges_from(edge_pairs)

    logger.info(
        "Built graph with %s nodes and %s edges",
        G.number_of_nodes(),
        G.number_of_edges(),
    )
    return G


def load_node_metadata(nodes_dir: Path) -> pl.DataFrame:
    """Load node id, label, and name from all node parquet files.

    Args:
        nodes_dir: Directory containing node parquet files.

    Returns:
        DataFrame with columns: id, label, name
    """
    frames: list[pl.DataFrame] = []
    for path in sorted(nodes_dir.glob("*.parquet")):
        df = pl.read_parquet(path)
        if df.height > 0:
            frames.append(
                df.select(
                    "id",
                    "label",
                    pl.col("properties").struct.field("name").alias("name"),
                )
            )

    result = pl.concat(frames)
    logger.info("Loaded metadata for %s nodes", result.height)
    return result


def compute_pagerank(G: nx.Graph, alpha: float = 0.85) -> dict[str, float]:
    """Compute PageRank scores for all nodes.

    Args:
        G: NetworkX graph.
        alpha: Damping factor (default 0.85).

    Returns:
        Dictionary mapping node ID to PageRank score.
    """
    logger.info("Computing PageRank with alpha=%s...", alpha)
    scores = nx.pagerank(G, alpha=alpha)
    logger.info("Computed PageRank for %s nodes", len(scores))
    return scores


def pagerank_to_dataframe(
    scores: dict[str, float],
    node_metadata: pl.DataFrame,
) -> pl.DataFrame:
    """Convert PageRank scores to a DataFrame with metadata.

    Args:
        scores: Dictionary mapping node ID to PageRank score.
        node_metadata: DataFrame with id, label, name columns.

    Returns:
        DataFrame with columns: id, label, name, pagerank
        (sorted by pagerank descending)
    """
    df = pl.DataFrame({"id": list(scores.keys()), "pagerank": list(scores.values())})

    return df.join(node_metadata, on="id", how="left").sort("pagerank", descending=True)
