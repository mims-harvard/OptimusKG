"""Degree distribution histogram (log-log).

Plots a histogram of node degrees across the entire knowledge graph on
log-log axes, revealing the power-law structure of the network.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl

from cli.commands.metrics.utils import build_degree_map, load_parquet_dir

from . import style  # noqa: F401
from .style import WARM_GRAY_SCALE, apply_axis_styling


def compute_data(nodes_dir: Path, edges_dir: Path) -> pl.DataFrame:
    """Compute the degree of every node in the knowledge graph.

    Returns a ``pl.DataFrame`` with a single column:
      - ``degree`` (Int64) – number of edges incident on the node
    """

    edges = load_parquet_dir(edges_dir)
    degree_map = build_degree_map(edges)
    return degree_map.select(pl.col("degree").cast(pl.Int64))


_BAR_COLOR = WARM_GRAY_SCALE["600"]


def render_plot(data: pl.DataFrame, out_path: Path) -> None:
    """Render a log-log degree-distribution histogram and save as PDF."""

    degrees = data["degree"].to_numpy().astype(float)

    # Log-spaced bins from 1 to the maximum degree.
    max_deg = degrees.max()
    bins = np.logspace(0, np.ceil(np.log10(max_deg)), 80)

    counts, edges = np.histogram(degrees, bins=bins)

    # Bar widths match the bin edges (variable width on log scale).
    widths = np.diff(edges)
    centres = edges[:-1]

    fig, ax = plt.subplots(figsize=(4, 3.5))

    ax.bar(
        centres,
        counts,
        width=widths,
        align="edge",
        color=_BAR_COLOR,
        edgecolor="none",
    )

    ax.set_xscale("log")
    ax.set_yscale("log")

    ax.set_xlabel("Degree", fontsize=7, fontweight="bold")
    ax.set_ylabel("Count", fontsize=7, fontweight="bold")
    ax.tick_params(axis="both", labelsize=6)
    ax.tick_params(which="minor", left=False, bottom=False)

    apply_axis_styling(ax)

    plt.savefig(out_path)
    plt.close(fig)
