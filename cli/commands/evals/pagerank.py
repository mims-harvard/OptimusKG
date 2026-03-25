"""Node centrality scores for the knowledge graph.

Computes a chosen centrality metric for all nodes in the knowledge graph,
outputs a ranked table with node metadata, and generates visualisations.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl

from .utils import (
    CentralityMetric,
    GraphMode,
    centrality_to_dataframe,
    compute_centrality,
    load_graph,
    load_node_metadata,
)

logger = logging.getLogger("cli")


def plot_centrality_by_type(
    df: pl.DataFrame,
    metric: CentralityMetric,
    out_path: Path,
) -> None:
    """Create and save bar chart of mean centrality score by node type.

    Args:
        df: Centrality DataFrame with label and centrality columns.
        metric: The centrality metric that was computed (used for axis labels).
        out_path: Path to save the figure.
    """
    by_type = (
        df.group_by("label")
        .agg(
            pl.col("centrality").mean().alias("mean_centrality"),
            pl.col("centrality").count().alias("count"),
        )
        .sort("mean_centrality", descending=True)
    )

    fig, ax = plt.subplots(figsize=(8, 5))

    labels = by_type["label"].to_list()[::-1]
    values = by_type["mean_centrality"].to_list()[::-1]
    colors = plt.cm.tab10.colors[: len(by_type)]

    ax.barh(labels, values, color=colors, edgecolor="black", linewidth=0.5)

    metric_label = metric.replace("_", " ").title()
    ax.set_xlabel(f"Mean {metric_label} Score", fontweight="bold")
    ax.set_ylabel("Node Type", fontweight="bold")
    ax.set_title(f"{metric_label} by Node Type", fontweight="bold")

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
    metric: CentralityMetric = "pagerank",
    graph_mode: GraphMode = "undirected",
) -> pl.DataFrame:
    """Run centrality analysis and save outputs.

    Args:
        nodes_path: Path to nodes.parquet file.
        edges_path: Path to edges.parquet file.
        out_dir: Directory to write outputs (CSV, figures).
        top_n: Number of top nodes to display in console.
        alpha: PageRank damping factor (only used when metric="pagerank").
        metric: Centrality metric to compute. One of:
            "pagerank", "degree", "betweenness", "closeness", "eigenvector".
            Default: "pagerank".
        graph_mode: Edge directionality mode passed to load_graph.
            "undirected" (default) adds reverse arcs for every edge;
            "directed" respects the undirected column only.

    Returns:
        Full centrality DataFrame.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build graph and compute centrality
    G, _, _ = load_graph(nodes_path, edges_path, graph_mode=graph_mode)
    node_metadata = load_node_metadata(nodes_path)
    scores = compute_centrality(G, metric=metric, alpha=alpha)
    df = (
        centrality_to_dataframe(scores, node_metadata)
        .with_row_index("rank", offset=1)
        .select("rank", "id", "label", "name", "centrality")
    )

    # Include the metric name in output filenames so multiple runs don't
    # overwrite each other
    csv_path = out_dir / f"{metric}.csv"
    df.write_csv(csv_path)
    logger.info("Saved CSV to %s", csv_path)

    plot_centrality_by_type(df, metric, out_dir / f"{metric}_by_type.pdf")

    logger.info("Top %d nodes by %s:\n%s", top_n, metric, df.head(top_n))

    return df
