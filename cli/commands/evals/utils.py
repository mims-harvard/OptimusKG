"""Shared utilities for evals commands."""

from __future__ import annotations

import logging
from pathlib import Path

import networkx as nx
import polars as pl

logger = logging.getLogger("cli")


def load_graph(
    nodes_path: Path,
    edges_path: Path,
) -> tuple[nx.MultiDiGraph, list[str], list[str]]:
    """Build a MultiDiGraph from node and edge parquet files.

    Reads nodes first to populate the graph, then reads edges. Each edge is
    stored with its relation ``label`` as an edge attribute. Because a
    ``MultiDiGraph`` supports parallel edges, multiple relation types between
    the same pair of nodes are kept as distinct edges rather than collapsed
    into a single entry.

    Undirected edges are represented by adding both the forward and the
    reverse directed edge (each carrying the same ``label``).

    Args:
        nodes_path: Path to nodes.parquet file.
        edges_path: Path to edges.parquet file.

    Returns:
        Tuple of:
        - G: Directed MultiDiGraph with ``label`` stored on every edge
        - node_types: Sorted list of unique node type labels
        - edge_types: Sorted list of unique edge type labels
    """
    G = nx.MultiDiGraph()

    # 1. Read nodes first
    nodes_df = pl.read_parquet(nodes_path)
    node_ids = nodes_df["id"].to_list()
    node_types_list = sorted(nodes_df["label"].unique().to_list())
    G.add_nodes_from(node_ids)

    logger.info(
        "Loaded %s nodes with %s node types: %s",
        G.number_of_nodes(),
        len(node_types_list),
        node_types_list,
    )

    # 2. Read edges
    edges_df = pl.read_parquet(edges_path)
    edge_types_list = sorted(edges_df["label"].unique().to_list())

    for row in edges_df.select("from", "to", "label", "undirected").iter_rows():
        from_id, to_id, label, is_undirected = row

        # Add forward edge, storing relation type as an attribute
        G.add_edge(from_id, to_id, label=label)

        # Add reverse edge for undirected relations so that successor queries
        # on either endpoint correctly surface the connection
        if is_undirected:
            G.add_edge(to_id, from_id, label=label)

    logger.info(
        "Loaded %s edges with %s edge types: %s",
        G.number_of_edges(),
        len(edge_types_list),
        edge_types_list,
    )

    return G, node_types_list, edge_types_list


def load_node_metadata(nodes_path: Path) -> pl.DataFrame:
    """Load node id, label, and name from nodes parquet file.

    Args:
        nodes_path: Path to nodes.parquet file.

    Returns:
        DataFrame with columns: id, label, name
    """
    df = pl.read_parquet(nodes_path)

    # Handle both struct and JSON string properties
    if df.schema["properties"] == pl.String:
        # Properties is JSON string - parse it
        result = df.select(
            "id",
            "label",
            pl.col("properties").str.json_path_match("$.name").alias("name"),
        )
    else:
        # Properties is struct - access field directly
        result = df.select(
            "id",
            "label",
            pl.col("properties").struct.field("name").alias("name"),
        )

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