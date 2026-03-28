"""Generate figures from PaperQA3 edge evaluation results.

Reads the polled-edges CSV produced by ``cli evals paperqa --action poll``
and produces a stacked bar plot of rating distributions, split into panels
for true and false edges and stratified by seed node type.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import polars as pl

logger = logging.getLogger("cli")

# Human-readable labels for compact node-type codes
NODE_TYPE_LABELS: dict[str, str] = {
    "ANA": "Anatomy",
    "BPO": "Biological Process",
    "CCO": "Cellular Component",
    "DIS": "Disease",
    "DRG": "Drug",
    "EXP": "Exposure",
    "GEN": "Gene / Protein",
    "MFN": "Molecular Function",
    "PHE": "Phenotype",
    "PWY": "Pathway",
}

# Colour palette — one per node type (tab10 has 10 colours, matching our 10 types)
_PALETTE = plt.cm.tab10.colors


def _run_id_from_path(path: Path) -> str:
    """Derive a run identifier from the input filename stem."""
    return path.stem


def run(
    input_path: Path,
    out_dir: Path | None = None,
) -> None:
    """Generate PaperQA rating bar plots.

    Args:
        input_path: Path to the polled-edges CSV (must contain columns
            ``seed_node_type``, ``is_true_edge``, ``rating``).
        out_dir: Directory to write the PDF. Defaults to the same directory
            as ``input_path``.
    """
    if not input_path.exists():
        msg = f"Input file not found: {input_path}"
        raise FileNotFoundError(msg)

    out_dir = out_dir or input_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    run_id = _run_id_from_path(input_path)
    out_path = out_dir / f"{run_id}_barplot.pdf"

    # ------------------------------------------------------------------
    # Load + clean
    # ------------------------------------------------------------------
    df = pl.read_csv(
        input_path,
        infer_schema_length=100000
    )

    required = {"seed_node_type", "is_true_edge", "rating"}
    missing = required - set(df.columns)
    if missing:
        msg = f"Input CSV is missing required columns: {missing}"
        raise ValueError(msg)

    # Coerce types: rating → int, is_true_edge → bool
    df = df.with_columns(
        pl.col("rating").cast(pl.Int32, strict=False),
        pl.col("is_true_edge").cast(pl.Boolean, strict=False),
    ).filter(
        pl.col("rating").is_not_null() & pl.col("is_true_edge").is_not_null()
    )

    if df.is_empty():
        logger.warning("No rows with valid rating + is_true_edge; nothing to plot.")
        return

    all_node_types = sorted(df["seed_node_type"].unique().drop_nulls().to_list())
    all_ratings = [1, 2, 3, 4, 5]
    color_map = {nt: _PALETTE[i % len(_PALETTE)] for i, nt in enumerate(all_node_types)}

    # ------------------------------------------------------------------
    # Build pivot: (edge_type, rating, node_type) → count
    # ------------------------------------------------------------------
    counts = (
        df.group_by("is_true_edge", "rating", "seed_node_type")
        .len()
        .rename({"len": "count"})
    )

    def _get_counts(is_true: bool, rating: int, node_type: str) -> int:
        row = counts.filter(
            (pl.col("is_true_edge") == is_true)
            & (pl.col("rating") == rating)
            & (pl.col("seed_node_type") == node_type)
        )
        return int(row["count"].sum()) if row.height > 0 else 0

    # ------------------------------------------------------------------
    # Plot
    # ------------------------------------------------------------------
    fig, axes = plt.subplots(1, 2, figsize=(10, 5))
    panels = [(False, "False edges"), (True, "True edges")]
 
    for ax, (is_true, panel_title) in zip(axes, panels):
        bottoms = [0] * len(all_ratings)
 
        for node_type in all_node_types:
            bar_heights = [_get_counts(is_true, r, node_type) for r in all_ratings]
            label = NODE_TYPE_LABELS.get(node_type, node_type)
            ax.bar(
                all_ratings,
                bar_heights,
                bottom=bottoms,
                color=color_map[node_type],
                label=label,
                edgecolor="black",
                linewidth=0.6,
                width=0.6,
            )
            bottoms = [b + h for b, h in zip(bottoms, bar_heights)]
 
        ax.set_title(panel_title, fontsize=13, pad=10)
        ax.set_xlabel("PaperQA Rating", fontsize=11)
        ax.set_ylabel("Count", fontsize=11)
        ax.set_xticks(all_ratings)
        ax.set_xticklabels(
            ["1\nNo evidence", "2\nWeak", "3\nModerate", "4\nStrong", "5\nVery strong"],
            fontsize=8.5,
        )
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
 
        # Remove top, bottom, and right spines; keep only left
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
 
        # Remove bottom ticks, keep left ticks
        ax.tick_params(axis="both", which="both", top=False, bottom=False, right=False, left=True)

 
    # Shared legend at the bottom of the figure, no frame
    handles, labels = axes[1].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        title="Seed node type",
        title_fontsize=9,
        fontsize=8.5,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.08),
        ncol=min(len(all_node_types), 5),
        frameon=False,
    )
 
    plt.tight_layout()
    plt.savefig(out_path, bbox_inches="tight")
    plt.close()

    logger.info("Saved bar plot to %s", out_path)
    print(f"Saved: {out_path}")
