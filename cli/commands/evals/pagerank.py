"""PageRank importance scores for the knowledge graph.

Computes PageRank centrality for all nodes in the knowledge graph,
outputs a ranked table with node metadata, and generates visualizations.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl

from .utils import (
    compute_pagerank,
    load_graph,
    load_node_metadata,
    pagerank_to_dataframe,
)

logger = logging.getLogger("cli")


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
    nodes_path: Path,
    edges_path: Path,
    out_dir: Path,
    top_n: int = 10,
    alpha: float = 0.85,
) -> pl.DataFrame:
    """Run PageRank analysis and save outputs.

    Args:
        nodes_path: Path to nodes.parquet file.
        edges_path: Path to edges.parquet file.
        out_dir: Directory to write outputs (CSV, figures).
        top_n: Number of top nodes to display in console.
        alpha: PageRank damping factor.

    Returns:
        Full PageRank DataFrame.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build graph and compute PageRank
    G, _, _, _ = load_graph(nodes_path, edges_path)
    node_metadata = load_node_metadata(nodes_path)
    scores = compute_pagerank(G, alpha=alpha)
    df = (
        pagerank_to_dataframe(scores, node_metadata)
        .with_row_index("rank", offset=1)
        .select("rank", "id", "label", "name", "pagerank")
    )

    # Save outputs
    csv_path = out_dir / "pagerank.csv"
    df.write_csv(csv_path)
    logger.info("Saved CSV to %s", csv_path)

    plot_pagerank_by_type(df, out_dir / "pagerank_by_type.pdf")

    # Log top N to console
    logger.info("Top %d nodes by PageRank:\n%s", top_n, df.head(top_n))

    return df
