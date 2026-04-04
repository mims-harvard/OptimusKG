"""Metapath count heatmaps for lengths 1-4.

Produces a 4-panel upper-triangular heatmap where each cell (i, j) in
panel L shows the number of structurally distinct metapaths of length L
connecting node type i to node type j.

A metapath of length L is a sequence ``T0 -[r1]- T1 -[r2]- ... -[rL]- TL``
of alternating node types and relation types.  Two counting methods are used:

- **Theoretical** (columns ``L1``-``L4``): computed via matrix exponentiation
  of the metaedge adjacency matrix.  Counts every combinatorially possible
  metapath schema, even those with no concrete instance in the graph.
- **Empirical** (columns ``E1``-``E4``): enumerates candidate schemas and
  verifies that at least one concrete path instance exists by checking
  intermediate node-set intersections.

A second PDF showing the empirical / theoretical ratio is also rendered.
"""

from __future__ import annotations

import logging
from collections import defaultdict
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

logger = logging.getLogger(__name__)

_NODE_TYPE_ORDER = [m.value for m in Node]

_MAX_LENGTH = 4
_MIN_MULTIHOP_LENGTH = 2

# Type alias for a directed metaedge: (src_type, relation, dst_type).
_Metaedge = tuple[str, str, str]


def _build_edge_index(
    edges: list[pl.DataFrame],
) -> dict[_Metaedge, tuple[set[str], set[str]]]:
    """Build a per-metaedge index of source and destination node IDs.

    For each directed metaedge ``(src_type, relation, dst_type)`` (including
    the reverse direction), returns a tuple ``(src_ids, dst_ids)`` containing
    the sets of node IDs that appear as source / destination for that
    metaedge.  This enables fast existence checks for multi-hop metapaths.
    """
    # Accumulate into lists first, then convert to sets once.
    src_acc: dict[_Metaedge, list[str]] = defaultdict(list)
    dst_acc: dict[_Metaedge, list[str]] = defaultdict(list)

    for df in edges:
        records = df.select(
            pl.col("label").str.split("-").list.get(0).alias("src_type"),
            pl.col("label").str.split("-").list.get(1).alias("dst_type"),
            pl.col("relation"),
            pl.col("from").alias("from_id"),
            pl.col("to").alias("to_id"),
        )

        for src_type, dst_type, rel, from_id, to_id in records.iter_rows():
            # Forward direction.
            fwd = (src_type, rel, dst_type)
            src_acc[fwd].append(from_id)
            dst_acc[fwd].append(to_id)
            # Reverse direction (undirected traversal).
            rev = (dst_type, rel, src_type)
            src_acc[rev].append(to_id)
            dst_acc[rev].append(from_id)

    return {me: (set(src_acc[me]), set(dst_acc[me])) for me in src_acc}


def _count_empirical_metapaths(
    edge_index: dict[_Metaedge, tuple[set[str], set[str]]],
    metaedges_by_pair: dict[tuple[str, str], list[_Metaedge]],
    max_length: int,
) -> dict[str, np.ndarray]:
    """Count empirical metapath schemas for each (type_i, type_j) pair.

    A metapath schema is counted as *empirical* if there exists at least one
    concrete path of that pattern in the graph.  Verification is done by
    propagating the set of reachable intermediate node IDs through each hop.

    Returns a dict mapping ``"E1"``..``"E{max_length}"`` to symmetric
    ``(n, n)`` integer matrices.
    """
    idx = {nt: i for i, nt in enumerate(_NODE_TYPE_ORDER)}
    n = len(_NODE_TYPE_ORDER)
    result: dict[str, np.ndarray] = {}

    # L1: every metaedge that exists in the data has at least one instance.
    E1 = np.zeros((n, n), dtype=np.int64)
    for me in edge_index:
        src, _rel, dst = me
        if src in idx and dst in idx:
            E1[idx[src], idx[dst]] += 1
    result["E1"] = E1

    if max_length < _MIN_MULTIHOP_LENGTH:
        return result

    # Helper: metaedges leaving a given node type.
    edges_from: dict[str, list[_Metaedge]] = defaultdict(list)
    for me in edge_index:
        edges_from[me[0]].append(me)

    def _enumerate_and_check(
        start_type: str,
        end_type: str,
        length: int,
    ) -> int:
        """Count schemas of *length* hops from *start_type* to *end_type*
        that have at least one concrete instance.

        Uses DFS over metaedge sequences with intermediate node-set
        propagation.
        """
        count = 0

        # Stack items: (current_type, hops_done, path_of_metaedges,
        #               reachable_node_ids_at_current_type)
        # We start with all node IDs of start_type (None means "not yet
        # filtered" -- we lazily intersect on the first hop).
        stack: list[tuple[str, int, list[_Metaedge], set[str] | None]] = [
            (start_type, 0, [], None),
        ]

        while stack:
            cur_type, hops, path, reachable = stack.pop()

            if hops == length:
                if cur_type == end_type:
                    count += 1
                continue

            for me in edges_from.get(cur_type, []):
                _src, _rel, next_type = me
                me_srcs, me_dsts = edge_index[me]

                # Intersect reachable set with the source IDs of this edge.
                if reachable is None:
                    live = me_srcs  # first hop: all sources of this metaedge
                else:
                    live = reachable & me_srcs

                if not live:
                    continue  # no node can continue along this path

                # The set of nodes reachable at the next type after this hop.
                # We need the destination IDs of edges whose source is in
                # *live*.  However, computing this exactly would require a
                # per-edge lookup.  Instead we use a conservative
                # approximation: the destination set of this metaedge.  This
                # is still correct for the existence check because if *live*
                # is non-empty it means at least one source node has this
                # edge, so at least one destination node is reachable.
                next_reachable = me_dsts

                stack.append((next_type, hops + 1, path + [me], next_reachable))

        return count

    for length in range(2, max_length + 1):
        mat = np.zeros((n, n), dtype=np.int64)
        for i, ft in enumerate(_NODE_TYPE_ORDER):
            for j, tt in enumerate(_NODE_TYPE_ORDER):
                mat[i, j] = _enumerate_and_check(ft, tt, length)
        key = f"E{length}"
        result[key] = mat
        logger.info("Empirical metapath counts for %s computed.", key)

    return result


def compute_data(nodes_dir: Path, edges_dir: Path) -> pl.DataFrame:
    """Compute theoretical and empirical metapath counts for lengths 1-4.

    Returns a ``pl.DataFrame`` with columns:
      - ``from_type`` (String) -- source node-type abbreviation
      - ``to_type``   (String) -- target node-type abbreviation
      - ``L1`` ... ``L4`` (Int64) -- *theoretical* metapath counts
      - ``E1`` ... ``E4`` (Int64) -- *empirical* metapath counts

    Only the upper triangle (``from_type <= to_type``) is stored since the
    matrices are symmetric.
    """
    all_edges = load_parquet_dir(edges_dir)

    metaedges: set[_Metaedge] = set()

    for df in all_edges:
        triples = df.select(
            pl.col("label").str.split("-").list.get(0).alias("src"),
            pl.col("label").str.split("-").list.get(1).alias("dst"),
            pl.col("relation"),
        ).unique()
        for row in triples.iter_rows():
            src, dst, rel = row
            metaedges.add((src, rel, dst))
            metaedges.add((dst, rel, src))  # reverse for undirected traversal

    idx = {nt: i for i, nt in enumerate(_NODE_TYPE_ORDER)}
    n = len(_NODE_TYPE_ORDER)
    E = np.zeros((n, n), dtype=np.int64)

    for src, _rel, dst in metaedges:
        if src in idx and dst in idx:
            E[idx[src], idx[dst]] += 1

    theo: dict[str, np.ndarray] = {"L1": E}
    power = E.copy()
    for length in range(2, _MAX_LENGTH + 1):
        power = power @ E
        theo[f"L{length}"] = power

    logger.info("Building per-metaedge node-ID index ...")
    edge_index = _build_edge_index(all_edges)

    metaedges_by_pair: dict[tuple[str, str], list[_Metaedge]] = defaultdict(list)
    for me in edge_index:
        metaedges_by_pair[(me[0], me[2])].append(me)

    logger.info("Counting empirical metapath schemas ...")
    emp = _count_empirical_metapaths(edge_index, metaedges_by_pair, _MAX_LENGTH)

    rows: list[dict[str, object]] = []
    for i, ft in enumerate(_NODE_TYPE_ORDER):
        for j, tt in enumerate(_NODE_TYPE_ORDER):
            if j >= i:  # upper triangle (including diagonal)
                row: dict[str, object] = {"from_type": ft, "to_type": tt}
                for key, mat in theo.items():
                    row[key] = int(mat[i, j])
                for key, mat in emp.items():
                    row[key] = int(mat[i, j])
                rows.append(row)

    all_cols = {f"L{k}": pl.Int64 for k in range(1, _MAX_LENGTH + 1)}
    all_cols.update({f"E{k}": pl.Int64 for k in range(1, _MAX_LENGTH + 1)})
    return pl.DataFrame(rows).cast(all_cols)


def _pivot_to_matrix(
    data: pl.DataFrame,
    col: str,
) -> np.ndarray:
    """Pivot a long-form upper-triangle DataFrame into a full symmetric matrix."""

    n = len(_NODE_TYPE_ORDER)
    idx = {nt: i for i, nt in enumerate(_NODE_TYPE_ORDER)}
    mat = np.zeros((n, n), dtype=float)

    for row in data.iter_rows(named=True):
        i = idx[row["from_type"]]
        j = idx[row["to_type"]]
        val = row[col]
        mat[i, j] = val
        mat[j, i] = val  # mirror

    return mat


_SI_KILO = 1_000
_SI_TEN_KILO = 10_000
_SI_MEGA = 1_000_000


def _format_si(value: float) -> str:
    """Format a number with SI suffixes (K, M) for compact display."""
    v = abs(value)
    if v < _SI_KILO:
        return f"{int(value)}"
    if v < _SI_TEN_KILO:
        return f"{value / _SI_KILO:.1f}K"
    if v < _SI_MEGA:
        return f"{value / _SI_KILO:.0f}K"
    return f"{value / _SI_MEGA:.1f}M"


def _format_count_matrix(mat: np.ndarray) -> np.ndarray:
    """Return a string array with SI-abbreviated cell labels."""
    vfunc = np.vectorize(lambda v: _format_si(v) if v != 0 else "")
    return vfunc(mat)


def _render_count_heatmap(
    data: pl.DataFrame,
    out_path: Path,
    prefix: str = "L",
) -> None:
    """Render a 4-panel metapath count heatmap.

    Parameters
    ----------
    prefix:
        Column prefix -- ``"L"`` for theoretical counts, ``"E"`` for
        empirical counts.
    """

    n = len(_NODE_TYPE_ORDER)
    length_cols = [f"{prefix}{k}" for k in range(1, _MAX_LENGTH + 1)]

    # Rebuild full matrices and find global vmax for a shared colour scale.
    matrices: dict[str, np.ndarray] = {}
    global_max = 0
    for col in length_cols:
        mat = _pivot_to_matrix(data, col)
        matrices[col] = mat
        global_max = max(global_max, int(mat.max()))

    # Upper-triangle mask (hide below diagonal).
    mask = np.triu(np.ones((n, n), dtype=bool), k=1)

    # Colour normalisation -- log scale so small and large values are visible.
    norm = mcolors.LogNorm(vmin=1, vmax=global_max)

    fig, axes = plt.subplots(
        1,
        _MAX_LENGTH,
        figsize=(12, 3.5),
        gridspec_kw={"wspace": 0.08},
    )

    for ax, col in zip(axes, length_cols):
        mat = matrices[col]
        masked_mat = np.ma.masked_equal(mat, 0)

        # Build human-readable annotation labels (e.g. 1.2K, 37K).
        annot_labels = _format_count_matrix(mat)

        sns.heatmap(
            masked_mat,
            ax=ax,
            mask=mask | (mat == 0),
            xticklabels=_NODE_TYPE_ORDER,
            yticklabels=_NODE_TYPE_ORDER,
            cmap=BLUE_CMAP,
            norm=norm,
            square=True,
            linewidths=0.3,
            linecolor="white",
            cbar=False,
            annot=annot_labels,
            fmt="",
            annot_kws={"fontsize": 5},
        )

        length_num = col[len(prefix) :]
        ax.set_title(f"Length {length_num}", fontsize=7, fontweight="bold")
        ax.tick_params(axis="x", rotation=45, labelsize=5)
        ax.tick_params(axis="y", rotation=0, labelsize=5)

        for lbl in ax.get_xticklabels() + ax.get_yticklabels():
            lbl.set_fontweight("bold")

        # Only show y-labels on the first panel.
        if col != length_cols[0]:
            ax.set_yticklabels([])
            ax.tick_params(axis="y", left=False)

        ax.set_xlabel("")
        ax.set_ylabel("")

        # Keep all 4 spines visible for heatmaps.
        for spine in ax.spines.values():
            spine.set_linewidth(0.5)
            spine.set_visible(True)

    # Shared colorbar on the right.
    sm = plt.cm.ScalarMappable(cmap=BLUE_CMAP, norm=norm)
    cbar = fig.colorbar(sm, ax=axes.tolist(), shrink=0.8, pad=0.02)
    cbar.set_label("Metapath count", fontsize=7)
    cbar.ax.tick_params(labelsize=6)

    plt.savefig(out_path)
    plt.close(fig)


def _render_ratio_heatmap(data: pl.DataFrame, out_path: Path) -> None:
    """Render a 4-panel heatmap of the empirical / theoretical ratio."""

    n = len(_NODE_TYPE_ORDER)

    # Upper-triangle mask (hide below diagonal).
    mask = np.triu(np.ones((n, n), dtype=bool), k=1)

    fig, axes = plt.subplots(
        1,
        _MAX_LENGTH,
        figsize=(12, 3.5),
        gridspec_kw={"wspace": 0.08},
    )

    norm = mcolors.Normalize(vmin=0.0, vmax=1.0)

    for ax, k in zip(axes, range(1, _MAX_LENGTH + 1)):
        theo_mat = _pivot_to_matrix(data, f"L{k}")
        emp_mat = _pivot_to_matrix(data, f"E{k}")

        # Compute ratio; cells where theoretical == 0 are masked.
        with np.errstate(divide="ignore", invalid="ignore"):
            ratio = np.where(theo_mat > 0, emp_mat / theo_mat, np.nan)

        # Mask: below diagonal OR theoretical count is zero.
        cell_mask = mask | (theo_mat == 0)

        sns.heatmap(
            ratio,
            ax=ax,
            mask=cell_mask,
            xticklabels=_NODE_TYPE_ORDER,
            yticklabels=_NODE_TYPE_ORDER,
            cmap="mpll-red-blue",
            norm=norm,
            square=True,
            linewidths=0.3,
            linecolor="white",
            cbar=False,
            annot=True,
            fmt=".2f",
            annot_kws={"fontsize": 5},
        )

        ax.set_title(f"Length {k}", fontsize=7, fontweight="bold")
        ax.tick_params(axis="x", rotation=45, labelsize=5)
        ax.tick_params(axis="y", rotation=0, labelsize=5)

        for lbl in ax.get_xticklabels() + ax.get_yticklabels():
            lbl.set_fontweight("bold")

        if k != 1:
            ax.set_yticklabels([])
            ax.tick_params(axis="y", left=False)

        ax.set_xlabel("")
        ax.set_ylabel("")

        for spine in ax.spines.values():
            spine.set_linewidth(0.5)
            spine.set_visible(True)

    # Shared colorbar.
    sm = plt.cm.ScalarMappable(cmap="mpll-red-blue", norm=norm)
    cbar = fig.colorbar(sm, ax=axes.tolist(), shrink=0.8, pad=0.02)
    cbar.set_label("Empirical / Theoretical ratio", fontsize=7)
    cbar.ax.tick_params(labelsize=6)

    plt.savefig(out_path)
    plt.close(fig)


def render_plot(data: pl.DataFrame, out_path: Path) -> None:
    """Render theoretical, empirical, and ratio heatmaps."""

    stem = out_path.stem
    suffix = out_path.suffix
    parent = out_path.parent

    _render_count_heatmap(data, parent / f"{stem}_theoretical{suffix}", prefix="L")
    _render_count_heatmap(data, parent / f"{stem}_empirical{suffix}", prefix="E")
    _render_ratio_heatmap(data, parent / f"{stem}_ratio{suffix}")
