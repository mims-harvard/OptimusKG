"""PageRank importance scores for the knowledge graph.

Computes PageRank centrality for all nodes in the knowledge graph,
outputs a ranked table with node metadata, and generates visualizations.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx
import polars as pl

from .utils import load_graph, load_node_metadata

logger = logging.getLogger("cli")


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
    """Convert PageRank scores to a ranked DataFrame with metadata.

    Args:
        scores: Dictionary mapping node ID to PageRank score.
        node_metadata: DataFrame with id, label, name columns.

    Returns:
        DataFrame with columns: rank, id, label, name, pagerank
    """
    df = pl.DataFrame({"id": list(scores.keys()), "pagerank": list(scores.values())})

    return (
        df.join(node_metadata, on="id", how="left")
        .sort("pagerank", descending=True)
        .with_row_index("rank", offset=1)
        .select("rank", "id", "label", "name", "pagerank")
    )


def plot_pagerank_by_type(df: pl.DataFrame, out_path: Path) -> None:
    """Create and save bar chart of mean PageRank by node type.

    Args:
        df: PageRank DataFrame with label column.
        out_path: Path to save the figure.
    """
    by_type = (
        df.group_by("label")
        .agg(
            pl.col("pagerank").mean().alias("mean_pagerank"),
            pl.col("pagerank").count().alias("count"),
        )
        .sort("mean_pagerank", descending=True)
    )

    fig, ax = plt.subplots(figsize=(8, 5))

    labels = by_type["label"].to_list()[::-1]
    values = by_type["mean_pagerank"].to_list()[::-1]
    colors = plt.cm.tab10.colors[: len(by_type)]

    ax.barh(labels, values, color=colors, edgecolor="black", linewidth=0.5)

    ax.set_xlabel("Mean PageRank Score", fontweight="bold")
    ax.set_ylabel("Node Type", fontweight="bold")
    ax.set_title("PageRank Importance by Node Type", fontweight="bold")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    logger.info("Saved figure to %s", out_path)


def run(
    nodes_dir: Path,
    edges_dir: Path,
    out_dir: Path,
    top_n: int = 10,
    alpha: float = 0.85,
) -> pl.DataFrame:
    """Run PageRank analysis and save outputs.

    Args:
        nodes_dir: Directory containing node parquet files.
        edges_dir: Directory containing edge parquet files.
        out_dir: Directory to write outputs (CSV, figures).
        top_n: Number of top nodes to display in console.
        alpha: PageRank damping factor.

    Returns:
        Full PageRank DataFrame.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build graph and compute PageRank
    G = load_graph(edges_dir)
    node_metadata = load_node_metadata(nodes_dir)
    scores = compute_pagerank(G, alpha=alpha)
    df = pagerank_to_dataframe(scores, node_metadata)

    # Save outputs
    csv_path = out_dir / "pagerank.csv"
    df.write_csv(csv_path)
    logger.info("Saved CSV to %s", csv_path)

    plot_pagerank_by_type(df, out_dir / "pagerank_by_type.pdf")

    # Log top N to console
    logger.info("Top %d nodes by PageRank:\n%s", top_n, df.head(top_n))

    return df
