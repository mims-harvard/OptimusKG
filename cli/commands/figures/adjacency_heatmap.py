"""Adjacency heatmap figure: node-type-to-node-type edge count matrix.

Produces a heatmap where rows and columns are node types and the cell
intensity represents the number of edges between them.
"""

from pathlib import Path

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import seaborn as sns

from cli.commands.metrics.utils import load_parquet_dir
from optimuskg.pipelines.silver.nodes.constants import Node

from . import style  # noqa: F401
from .style import BLUE_CMAP

_NODE_TYPE_ORDER = [member.value for member in Node]


def _extract_type_counts(df: pl.DataFrame) -> pl.DataFrame:
    """Extract (from_type, to_type, count) rows from an edge DataFrame.

    Splits the ``label`` column (e.g. ``"DIS-GEN"``) on ``"-"`` to get the
    source and target node-type abbreviations.
    For undirected edges the reverse pair is also emitted so the resulting
    matrix is symmetric.
    """
    typed = df.with_columns(
        pl.col("label").str.split("-").list.get(0).alias("from_type"),
        pl.col("label").str.split("-").list.get(1).alias("to_type"),
    )

    directed = typed.group_by("from_type", "to_type").agg(pl.len().alias("edge_count"))

    # For undirected edges, add the reverse pair (skip self-loops to avoid
    # double-counting on the diagonal).
    if "undirected" in df.columns:
        undirected_edges = typed.filter(pl.col("undirected"))
        if undirected_edges.height > 0:
            reverse = (
                undirected_edges.group_by("from_type", "to_type")
                .agg(pl.len().alias("edge_count"))
                .rename({"from_type": "to_type", "to_type": "from_type"})
                .select("from_type", "to_type", "edge_count")
                .filter(pl.col("from_type") != pl.col("to_type"))
            )
            directed = pl.concat([directed, reverse])

    return directed


def compute_data(nodes_dir: Path, edges_dir: Path) -> pl.DataFrame:
    """Compute the node-type adjacency matrix from gold edge parquet files.

    Returns a ``pl.DataFrame`` with a ``node_type`` column (row label) and
    one ``Int64`` column per node-type abbreviation containing edge counts.
    """
    edges = load_parquet_dir(edges_dir)

    parts = [_extract_type_counts(df) for df in edges]
    if not parts:
        # Return an empty matrix with the correct shape.
        return pl.DataFrame(
            {"node_type": _NODE_TYPE_ORDER}
            | {nt: [0] * len(_NODE_TYPE_ORDER) for nt in _NODE_TYPE_ORDER}
        )

    counts = (
        pl.concat(parts)
        .group_by("from_type", "to_type")
        .agg(pl.col("edge_count").sum())
    )

    # Pivot into a wide matrix.
    matrix = counts.pivot(
        on="to_type",
        index="from_type",
        values="edge_count",
    ).rename({"from_type": "node_type"})

    # Ensure every node type appears as both a row and a column.
    for nt in _NODE_TYPE_ORDER:
        if nt not in matrix.columns:
            matrix = matrix.with_columns(pl.lit(0).alias(nt))

    existing_row_types = set(matrix["node_type"].to_list())
    missing_rows = [nt for nt in _NODE_TYPE_ORDER if nt not in existing_row_types]
    if missing_rows:
        filler = pl.DataFrame(
            {"node_type": missing_rows}
            | {col: [0] * len(missing_rows) for col in _NODE_TYPE_ORDER}
        )
        matrix = pl.concat([matrix, filler], how="diagonal_relaxed")

    # Fill nulls (no edges between those types) with 0 and reorder.
    matrix = matrix.fill_null(0)
    matrix = matrix.select(["node_type", *_NODE_TYPE_ORDER])

    # Sort rows to match the canonical enum order.
    order_map = {nt: i for i, nt in enumerate(_NODE_TYPE_ORDER)}
    matrix = (
        matrix.with_columns(
            pl.col("node_type").replace_strict(order_map).alias("_order")
        )
        .sort("_order")
        .drop("_order")
    )

    return matrix.cast({col: pl.Int64 for col in _NODE_TYPE_ORDER})


_MILLION = 1_000_000
_THOUSAND = 1_000


def _format_count(value: int) -> str:
    """Format an integer count for heatmap cell annotation."""
    if value == 0:
        return ""
    if value >= _MILLION:
        return f"{value / _MILLION:.1f}M"
    if value >= _THOUSAND:
        return f"{value / _THOUSAND:.0f}K"
    return str(value)


def render_plot(data: pl.DataFrame, out_path: Path) -> None:
    """Render the adjacency heatmap and save as PDF."""

    row_labels: list[str] = data["node_type"].cast(pl.Utf8).to_list()
    matrix = data.drop("node_type").to_numpy()

    # Build annotation strings.
    annotations = np.vectorize(_format_count)(matrix)

    # Use a log-normalised colormap; mask zeros so they render as the
    # background colour rather than the lowest colour.
    masked = np.ma.masked_equal(matrix, 0)
    vmin = max(masked.min(), 1) if masked.count() > 0 else 1
    vmax = matrix.max() if matrix.max() > 0 else 1
    norm = mcolors.LogNorm(vmin=vmin, vmax=vmax)

    fig, ax = plt.subplots(figsize=(5, 4.5))

    sns.heatmap(
        matrix,
        ax=ax,
        xticklabels=row_labels,
        yticklabels=row_labels,
        annot=annotations,
        fmt="",
        annot_kws={"fontsize": 5},
        cmap=BLUE_CMAP,
        norm=norm,
        square=True,
        linewidths=0.5,
        linecolor="white",
        cbar_kws={"shrink": 0.5},
        mask=(matrix == 0),
    )

    # Style the colorbar.
    cbar = ax.collections[0].colorbar
    cbar.set_label("Edge count", fontsize=7)
    cbar.ax.tick_params(labelsize=6)

    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.tick_params(axis="x", rotation=0, labelsize=7)
    ax.tick_params(axis="y", rotation=0, labelsize=7)

    # Bold row/column labels.
    for lbl in ax.get_xticklabels() + ax.get_yticklabels():
        lbl.set_fontweight("bold")

    # Heatmaps keep all four spines visible (unlike scatter plots).
    ax.grid(False)
    for spine in ax.spines.values():
        spine.set_linewidth(0.5)

    plt.savefig(out_path)
    plt.close(fig)
