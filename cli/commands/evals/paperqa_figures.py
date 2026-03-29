"""Generate figures from PaperQA3 edge evaluation results.

Reads the polled-edges CSV produced by ``cli evals paperqa --action poll``
and produces two figures:

* ``<run_id>_barplot.pdf`` — two panels (false / true edges), stacked by seed
  node type, node types ordered by prevalence.
* ``<run_id>_grouped_barplot.pdf`` — one panel per seed node type (2 cols × 5
  rows), stacked by relation type for true edges (colored) + false edges as a
  single light-gray segment, relation types ordered by prevalence.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import polars as pl

logger = logging.getLogger("cli")

# Human-readable labels for compact node-type codes
NODE_TYPE_LABELS: dict[str, str] = {
    "ANA": "Anatomy",
    "BPO": "Biological process",
    "CCO": "Cellular component",
    "DIS": "Disease",
    "DRG": "Drug",
    "EXP": "Exposure",
    "GEN": "Gene or protein",
    "MFN": "Molecular function",
    "PHE": "Phenotype",
    "PWY": "Pathway",
}

_PALETTE = [
    "#516FD9",  # Royal Blue
    "#7EACF5",  # Sky Blue
    "#69C39C",  # Mint
    "#6FA430",  # Green
    "#E7C454",  # Yellow
    "#EDB453",  # Amber
    "#ED9353",  # Orange
    "#DA3546",  # Red
    "#9B7DF1",  # Purple
    "#838E9F",  # Gray
]

_FALSE_GRAY = "#D0D0D0"
_ALL_RATINGS = [1, 2, 3, 4, 5]
_RATING_LABELS = ["No evidence", "Weak", "Moderate", "Strong", "Very strong"]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _run_id_from_path(path: Path) -> str:
    return path.stem


def _load_df(input_path: Path) -> pl.DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pl.read_csv(input_path, infer_schema_length=100000)

    required = {"seed_node_type", "is_true_edge", "rating"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input CSV is missing required columns: {missing}")

    return (
        df.with_columns(
            pl.col("rating").cast(pl.Int32, strict=False),
            pl.col("is_true_edge").cast(pl.Boolean, strict=False),
        ).filter(
            pl.col("rating").is_not_null() & pl.col("is_true_edge").is_not_null()
        )
    )


def _by_prevalence(df: pl.DataFrame, col: str) -> list[str]:
    """Return unique values of `col` ordered by descending row count."""
    return (
        df.group_by(col)
        .len()
        .sort("len", descending=True)
        [col]
        .drop_nulls()
        .to_list()
    )


def _node_types_by_pct_delta(df: pl.DataFrame) -> list[str]:
    """Order seed node types by (pct_rating_5 - pct_rating_2) for true edges, descending.

    Nodes where 'Very strong' evidence dominates over 'Weak' evidence appear first.
    """
    true_df = df.filter(pl.col("is_true_edge"))

    counts = (
        true_df
        .group_by("seed_node_type", "rating")
        .len()
        .rename({"len": "count"})
    )
    totals = (
        true_df
        .group_by("seed_node_type")
        .len()
        .rename({"len": "total"})
    )

    node_types = totals["seed_node_type"].drop_nulls().to_list()
    deltas: dict[str, float] = {}
    for nt in node_types:
        total = int(totals.filter(pl.col("seed_node_type") == nt)["total"][0])
        c5 = counts.filter((pl.col("seed_node_type") == nt) & (pl.col("rating") == 5))
        c2 = counts.filter((pl.col("seed_node_type") == nt) & (pl.col("rating") == 2))
        pct5 = (int(c5["count"].sum()) if c5.height > 0 else 0) / total
        pct2 = (int(c2["count"].sum()) if c2.height > 0 else 0) / total
        deltas[nt] = pct5 - pct2

    return sorted(node_types, key=lambda nt: deltas[nt], reverse=True)


def _ax_style(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(axis="both", which="both", top=False, bottom=False, right=False, left=True)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))


# ---------------------------------------------------------------------------
# Figure 1: two-panel barplot (false | true), stacked by seed node type
# ---------------------------------------------------------------------------

def _plot_barplot(df: pl.DataFrame, out_dir: Path, run_id: str) -> None:
    # Node types ordered by prevalence
    node_types = _by_prevalence(df, "seed_node_type")
    color_map = {nt: _PALETTE[i % len(_PALETTE)] for i, nt in enumerate(node_types)}

    counts = (
        df.group_by("is_true_edge", "rating", "seed_node_type")
        .len()
        .rename({"len": "count"})
    )

    def _get(is_true: bool, rating: int, node_type: str) -> int:
        row = counts.filter(
            (pl.col("is_true_edge") == is_true)
            & (pl.col("rating") == rating)
            & (pl.col("seed_node_type") == node_type)
        )
        return int(row["count"].sum()) if row.height > 0 else 0

    fig, axes = plt.subplots(1, 2, figsize=(10, 5))

    for ax, (is_true, panel_title) in zip(axes, [(False, "False edges"), (True, "True edges")]):
        bottoms = [0] * len(_ALL_RATINGS)
        for node_type in node_types:
            heights = [_get(is_true, r, node_type) for r in _ALL_RATINGS]
            ax.bar(
                _ALL_RATINGS, heights, bottom=bottoms,
                color=color_map[node_type],
                label=NODE_TYPE_LABELS.get(node_type, node_type),
                edgecolor="black", linewidth=0.6, width=0.6,
            )
            bottoms = [b + h for b, h in zip(bottoms, heights)]

        ax.set_title(panel_title, fontsize=13, pad=10)
        ax.set_xlabel("")
        ax.set_ylabel("Count", fontsize=11)
        ax.set_xticks(_ALL_RATINGS)
        ax.set_xticklabels(_RATING_LABELS)
        _ax_style(ax)

    handles, labels = axes[1].get_legend_handles_labels()
    fig.legend(
        handles, labels,
        title="Seed node type", title_fontsize=9, fontsize=8.5,
        loc="lower center", bbox_to_anchor=(0.5, -0.12),
        ncol=min(len(node_types), 5), frameon=False,
    )

    plt.tight_layout()
    out_path = out_dir / f"{run_id}_barplot.pdf"
    plt.savefig(out_path, bbox_inches="tight")
    plt.close()
    logger.info("Saved bar plot to %s", out_path)
    print(f"Saved: {out_path}")


# ---------------------------------------------------------------------------
# Figure 2: grouped barplot, faceted by seed node type (2 cols × 5 rows)
#
# Within each panel, each rating position has two side-by-side bars:
#   left  — true edges, stacked by relation type (global color scheme)
#   right — false edges, single light-gray bar
# ---------------------------------------------------------------------------

def _plot_grouped_barplot(df: pl.DataFrame, out_dir: Path, run_id: str) -> None:
    if "relation_type" not in df.columns:
        raise ValueError("Input CSV is missing required column: relation_type")

    # Node types ordered by pct(rating=5) - pct(rating=2) for true edges
    node_types = _node_types_by_pct_delta(df)

    bar_w = 0.38
    true_x  = [r - bar_w / 2 for r in _ALL_RATINGS]  # left bar  = true edges
    false_x = [r + bar_w / 2 for r in _ALL_RATINGS]  # right bar = false edges

    n_cols, n_rows = 2, 5
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(9, n_rows * 3))

    for idx, node_type in enumerate(node_types):
        ax = axes[idx // n_cols][idx % n_cols]
        panel_df = df.filter(pl.col("seed_node_type") == node_type)

        panel_counts = (
            panel_df
            .group_by("is_true_edge", "rating", "relation_type")
            .len()
            .rename({"len": "count"})
        )

        # Color map scoped to this panel: relation types ordered by prevalence within the panel
        relation_types = _by_prevalence(panel_df.filter(pl.col("is_true_edge")), "relation_type")
        rel_color_map = {rt: _PALETTE[i % len(_PALETTE)] for i, rt in enumerate(relation_types)}

        def _get(is_true: bool, rating: int, rel_type: str | None = None) -> int:
            q = panel_counts.filter(
                (pl.col("is_true_edge") == is_true) & (pl.col("rating") == rating)
            )
            if rel_type is not None:
                q = q.filter(pl.col("relation_type") == rel_type)
            return int(q["count"].sum()) if q.height > 0 else 0

        legend_handles: list[mpatches.Patch] = []
        legend_labels: list[str] = []

        # Left bar: true edges stacked by relation type (global prevalence order + colors)
        true_bottoms = [0] * len(_ALL_RATINGS)
        for rel_type in relation_types:
            heights = [_get(True, r, rel_type) for r in _ALL_RATINGS]
            if sum(heights) == 0:
                continue
            color = rel_color_map[rel_type]
            ax.bar(
                true_x, heights, bottom=true_bottoms,
                color=color, edgecolor="white", linewidth=0.4, width=bar_w,
            )
            legend_handles.append(mpatches.Patch(facecolor=color, label=rel_type))
            legend_labels.append(rel_type)
            true_bottoms = [b + h for b, h in zip(true_bottoms, heights)]

        # Right bar: false edges — single light-gray bar (no stacking)
        false_heights = [_get(False, r) for r in _ALL_RATINGS]
        if sum(false_heights) > 0:
            ax.bar(
                false_x, false_heights,
                color=_FALSE_GRAY, edgecolor="white", linewidth=0.4, width=bar_w,
            )
            legend_handles.append(mpatches.Patch(facecolor=_FALSE_GRAY, label="False edges"))
            legend_labels.append("False edges")

        ax.set_title(NODE_TYPE_LABELS.get(node_type, node_type), fontsize=10)
        ax.set_xticks(_ALL_RATINGS)
        # ax.set_xticklabels(_RATING_LABELS, fontsize=7.5, rotation=30, ha="right")
        ax.set_xticklabels(_RATING_LABELS, fontsize=8)
        ax.set_ylabel("Count", fontsize=9)
        _ax_style(ax)

        # Per-panel legend underneath
        ax.legend(
            legend_handles, legend_labels,
            fontsize=7, ncol=3,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.38),
            frameon=False,
        )

    # Hide unused panels if fewer than 10 node types
    for idx in range(len(node_types), n_rows * n_cols):
        axes[idx // n_cols][idx % n_cols].set_visible(False)

    plt.tight_layout(h_pad=0.5)
    out_path = out_dir / f"{run_id}_grouped_barplot.pdf"
    plt.savefig(out_path, bbox_inches="tight")
    plt.close()
    logger.info("Saved grouped bar plot to %s", out_path)
    print(f"Saved: {out_path}")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(
    input_path: Path,
    out_dir: Path | None = None,
) -> None:
    """Generate PaperQA rating bar plots.

    Args:
        input_path: Path to the polled-edges CSV (must contain columns
            ``seed_node_type``, ``is_true_edge``, ``rating``, ``relation_type``).
        out_dir: Directory to write PDFs. Defaults to the same directory as
            ``input_path``.
    """
    out_dir = out_dir or input_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _load_df(input_path)
    if df.is_empty():
        logger.warning("No rows with valid rating + is_true_edge; nothing to plot.")
        return

    run_id = _run_id_from_path(input_path)

    _plot_barplot(df, out_dir, run_id)
    _plot_grouped_barplot(df, out_dir, run_id)
