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
) -> tuple[nx.Graph, dict[tuple[str, str], str], list[str], list[str]]:
    """Build an undirected NetworkX graph from node and edge parquet files.

    Reads nodes first to populate the graph, then reads edges.
    Also builds a lookup for edge types (relation labels).

    Args:
        nodes_path: Path to nodes.parquet file.
        edges_path: Path to edges.parquet file.

    Returns:
        Tuple of:
        - G: Undirected NetworkX graph
        - edge_type_lookup: Dict mapping (from_id, to_id) -> edge label
        - node_types: Sorted list of unique node type labels
        - edge_types: Sorted list of unique edge type labels
    """
    G = nx.Graph()

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

    edge_type_lookup: dict[tuple[str, str], str] = {}
    for row in edges_df.select("from", "to", "label").iter_rows():
        from_id, to_id, label = row
        G.add_edge(from_id, to_id)
        # Store both directions for undirected lookup
        edge_type_lookup[(from_id, to_id)] = label
        edge_type_lookup[(to_id, from_id)] = label

    logger.info(
        "Loaded %s edges with %s edge types: %s",
        G.number_of_edges(),
        len(edge_types_list),
        edge_types_list,
    )

    return G, edge_type_lookup, node_types_list, edge_types_list


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
