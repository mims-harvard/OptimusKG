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
import matplotlib.figure as mfigure
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from matplotlib.axes import Axes

from cli.commands.metrics.utils import load_parquet_dir
from optimuskg.pipelines.silver.nodes.constants import Node

from . import style  # noqa: F401
from .style import BLUE_CMAP, WARM_GRAY_SCALE, apply_axis_styling

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

    node_counts: dict[str, int] = defaultdict(int)
    for df in load_parquet_dir(nodes_dir):
        label = df["label"][0]
        if label is None:
            continue
        mapped_label: str = _EDGE_TO_NODE_LABEL.get(str(label), str(label))

        # Count unique nodes per node type (not raw rows), since a file can
        # contain duplicates in some intermediate/export scenarios.
        if "id" in df.columns:
            unique_nodes = df["id"].n_unique()
        else:
            unique_nodes = df.height

        # Aggregate in case multiple parquet files map to the same node label.
        node_counts[mapped_label] += int(unique_nodes)

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
        if src_type is None or dst_type is None:
            continue

        # Map edge labels to node labels
        mapped_src: str = _EDGE_TO_NODE_LABEL.get(str(src_type), str(src_type))
        mapped_dst: str = _EDGE_TO_NODE_LABEL.get(str(dst_type), str(dst_type))

        # Accumulate edge counts – each edge is incident on both src and dst
        edge_counts[mapped_src] += n_rows
        if mapped_src != mapped_dst:
            edge_counts[mapped_dst] += n_rows

        # Accumulate metaedge sets
        for row in unique_metaedges:
            src = row["src"]
            dst = row["dst"]
            if src is None or dst is None:
                continue
            ms: str = _EDGE_TO_NODE_LABEL.get(str(src), str(src))
            md: str = _EDGE_TO_NODE_LABEL.get(str(dst), str(dst))
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


_COL_POSITIONS = [0.0, 0.35, 0.70]

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
        cmap=BLUE_CMAP,
        norm=norm,
        edgecolors=WARM_GRAY_SCALE["100"],
        linewidths=0.4,
        zorder=3,
        clip_on=False,
    )

    cbar = fig.colorbar(sc, ax=ax, shrink=0.5, pad=0.02)
    cbar.set_label("Avg. degree", fontsize=7)
    cbar.ax.tick_params(labelsize=6)
    cbar.ax.set_frame_on(False)
    for spine in ("left", "right", "top", "bottom"):
        cbar.ax.spines[spine].set_visible(False)

    # Bubble radius in display points (scatter size is area in pt^2).
    radii_pt = np.sqrt(sizes) / 2
    for i, label in enumerate(node_types):
        y_frac = (np.log10(y[i]) - log_y_min) / (log_y_max - log_y_min)
        gap = float(4 + radii_pt[i])
        if y_frac < 0.15:
            off_y = gap  # above
            va = "bottom"
        else:
            off_y = -gap  # below
            va = "top"
        ax.annotate(
            label,
            (x[i], y[i]),
            textcoords="offset points",
            xytext=(0.0, off_y),
            color=WARM_GRAY_SCALE["900"],
            ha="center",
            va=va,
            fontsize=7,
            fontweight="semibold",
            clip_on=False,
            annotation_clip=False,
            zorder=5,
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
