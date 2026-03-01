"""Metaedge bubble plot: log-log scatter of nodes vs edges per node type.

Each bubble represents a node type.  The x-axis is the number of nodes, the
y-axis is the number of edges, the bubble size encodes the number of distinct
metaedges (``SRC-RELATION-DST`` triples) the node type participates in, and
the colour encodes average degree (edges per node).
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from cli.commands.metrics.utils import load_parquet_dir
from optimuskg.pipelines.silver.nodes.constants import Node

from . import style  # noqa: F401
from .style import apply_axis_styling

# Edges use PRO in their labels; remap to GEN so all figures consistently
# show the Gene node type.  PRO is excluded from the order.
_EDGE_TO_NODE_LABEL = {"PRO": "GEN"}

_NODE_TYPE_ORDER = [member.value for member in Node if member is not Node.PROTEIN]


def compute_data(nodes_dir: Path, edges_dir: Path) -> pl.DataFrame:
    """Compute per-node-type statistics for the bubble plot.

    Returns a ``pl.DataFrame`` with columns:
      - ``node_type``      (String)   – 3-letter abbreviation
      - ``node_count``     (Int64)    – number of nodes of that type
      - ``edge_count``     (Int64)    – total edges incident on that type
      - ``metaedge_count`` (Int64)    – distinct metaedges for that type
      - ``avg_degree``     (Float64)  – average degree (edge_count / node_count)
    """

    node_counts: dict[str, int] = {}
    for df in load_parquet_dir(nodes_dir):
        label = df["label"][0]
        node_counts[label] = df.height

    edge_counts: dict[str, int] = defaultdict(int)
    # metaedge_sets stores the set of unique metaedge strings per node type
    metaedge_sets: dict[str, set[str]] = defaultdict(set)

    for df in load_parquet_dir(edges_dir):
        typed = df.select(
            pl.col("label").str.split("-").list.get(0).alias("src"),
            pl.col("label").str.split("-").list.get(1).alias("dst"),
            pl.col("relation"),
        )

        # Unique metaedges in this file
        unique_metaedges = typed.select("src", "relation", "dst").unique().to_dicts()

        n_rows = df.height
        # Determine the (single) src and dst types in this file
        src_type = typed["src"][0]
        dst_type = typed["dst"][0]

        # Map edge labels to node labels
        mapped_src = _EDGE_TO_NODE_LABEL.get(src_type, src_type)
        mapped_dst = _EDGE_TO_NODE_LABEL.get(dst_type, dst_type)

        # Accumulate edge counts – each edge is incident on both src and dst
        edge_counts[mapped_src] += n_rows
        if mapped_src != mapped_dst:
            edge_counts[mapped_dst] += n_rows

        # Accumulate metaedge sets
        for row in unique_metaedges:
            ms = _EDGE_TO_NODE_LABEL.get(row["src"], row["src"])
            md = _EDGE_TO_NODE_LABEL.get(row["dst"], row["dst"])
            metaedge = f"{ms}-{row['relation']}-{md}"
            metaedge_sets[ms].add(metaedge)
            metaedge_sets[md].add(metaedge)

    rows: list[dict[str, object]] = []
    for nt in _NODE_TYPE_ORDER:
        nc = node_counts.get(nt, 0)
        ec = edge_counts.get(nt, 0)
        mc = len(metaedge_sets.get(nt, set()))
        # Only include node types that have both nodes and edges
        if nc > 0 and ec > 0:
            rows.append(
                {
                    "node_type": nt,
                    "node_count": nc,
                    "edge_count": ec,
                    "metaedge_count": mc,
                    "avg_degree": ec / nc,
                }
            )

    return pl.DataFrame(rows).cast(
        {
            "node_count": pl.Int64,
            "edge_count": pl.Int64,
            "metaedge_count": pl.Int64,
            "avg_degree": pl.Float64,
        }
    )


def _scale_sizes(
    values: np.ndarray,
    *,
    min_size: float = 40,
    max_size: float = 550,
) -> np.ndarray:
    """Linearly scale *values* into marker-area range [min_size, max_size]."""
    lo, hi = values.min(), values.max()
    if hi == lo:
        return np.full_like(values, (min_size + max_size) / 2, dtype=float)
    return min_size + (max_size - min_size) * (values - lo) / (hi - lo)


def render_plot(data: pl.DataFrame, out_path: Path) -> None:
    """Render the metaedge bubble plot and save as PDF."""

    node_types = data["node_type"].to_list()
    x = data["node_count"].to_numpy().astype(float)
    y = data["edge_count"].to_numpy().astype(float)
    metaedge_counts = data["metaedge_count"].to_numpy().astype(float)
    avg_degree = data["avg_degree"].to_numpy().astype(float)

    sizes = _scale_sizes(metaedge_counts)

    # Log-normalise colour so the wide range of avg_degree is visible.
    norm = mcolors.LogNorm(vmin=avg_degree.min(), vmax=avg_degree.max())

    # Axis limits (defined early so label placement can reference them).
    x_lo, x_hi = 5e2, 1e5
    y_lo, y_hi = 5e3, 5e7
    log_y_min, log_y_max = np.log10(y_lo), np.log10(y_hi)

    fig, ax = plt.subplots(figsize=(4, 3.5))

    sc = ax.scatter(
        x,
        y,
        s=sizes,
        c=avg_degree,
        cmap="mpll-blue",
        norm=norm,
        edgecolors="black",
        linewidths=0.4,
        zorder=3,
        clip_on=False,
    )

    cbar = fig.colorbar(sc, ax=ax, shrink=0.8, pad=0.02)
    cbar.set_label("Avg. degree", fontsize=7)
    cbar.ax.tick_params(labelsize=6)

    # Work in display (pixel) coordinates so offsets are resolution-independent.
    fig.canvas.draw()  # force a layout pass so transData is accurate

    # Bubble radius in display points (scatter size is area in pt^2).
    radii_pt = np.sqrt(sizes) / 2

    # Convert data positions to display coordinates.
    display_pts = np.column_stack(
        [ax.transData.transform((xi, yi)) for xi, yi in zip(x, y)]
    ).T  # shape (N, 2)

    # Approximate label dimensions in display points.
    _LABEL_H = 10  # approx height of one label line at fontsize 8
    _LABEL_W = 22  # approx half-width of a 3-char bold label

    # Initial placement: below each bubble by default.
    label_cx = display_pts[:, 0].copy()
    label_cy = display_pts[:, 1].copy()
    va_list: list[str] = []

    for i in range(len(node_types)):
        y_frac = (np.log10(y[i]) - log_y_min) / (log_y_max - log_y_min)
        gap = 4 + radii_pt[i]
        if y_frac < 0.15:
            label_cy[i] += gap  # above
            va_list.append("bottom")
        else:
            label_cy[i] -= gap  # below
            va_list.append("top")

    # Collision resolution: if two label bounding boxes overlap, nudge them.
    def _boxes_overlap(i: int, j: int) -> bool:
        """Check if the approximate bounding boxes of labels i and j overlap."""
        dy_i = _LABEL_H if va_list[i] == "top" else -_LABEL_H
        dy_j = _LABEL_H if va_list[j] == "top" else -_LABEL_H
        # box: (left, right, bottom, top)
        b_i = (
            label_cx[i] - _LABEL_W,
            label_cx[i] + _LABEL_W,
            min(label_cy[i], label_cy[i] - dy_i),
            max(label_cy[i], label_cy[i] - dy_i),
        )
        b_j = (
            label_cx[j] - _LABEL_W,
            label_cx[j] + _LABEL_W,
            min(label_cy[j], label_cy[j] - dy_j),
            max(label_cy[j], label_cy[j] - dy_j),
        )
        return (
            b_i[0] < b_j[1] and b_i[1] > b_j[0] and b_i[2] < b_j[3] and b_i[3] > b_j[2]
        )

    for i in range(len(node_types)):
        for j in range(i + 1, len(node_types)):
            if _boxes_overlap(i, j):
                # Nudge the label with the smaller bubble to the opposite
                # vertical side, or shift it right if both are similar size.
                nudge_idx = j if sizes[i] >= sizes[j] else i
                gap = 4 + radii_pt[nudge_idx]
                if va_list[nudge_idx] == "top":
                    # Move above
                    label_cy[nudge_idx] = display_pts[nudge_idx, 1] + gap
                    va_list[nudge_idx] = "bottom"
                else:
                    # Move below
                    label_cy[nudge_idx] = display_pts[nudge_idx, 1] - gap
                    va_list[nudge_idx] = "top"
                # If they still overlap after vertical flip, shift right
                if _boxes_overlap(i, j):
                    label_cx[nudge_idx] += _LABEL_W * 1.5

    # Convert back to offset-from-data-point in display points.
    for i, label in enumerate(node_types):
        off_x = label_cx[i] - display_pts[i, 0]
        off_y = label_cy[i] - display_pts[i, 1]
        ax.annotate(
            label,
            (x[i], y[i]),
            textcoords="offset points",
            xytext=(off_x, off_y),
            ha="center",
            va=va_list[i],
            fontsize=7,
            fontweight="bold",
        )

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlim(x_lo, x_hi)
    ax.set_ylim(y_lo, y_hi)

    ax.set_xlabel("Nodes", fontsize=7, fontweight="bold")
    ax.set_ylabel("Edges", fontsize=7, fontweight="bold")

    apply_axis_styling(ax)
    ax.tick_params(axis="both", labelsize=6)
    ax.tick_params(which="minor", left=False, bottom=False)

    plt.savefig(out_path)
    plt.close(fig)
