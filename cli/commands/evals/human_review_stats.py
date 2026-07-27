"""Publication-ready statistics and figures from expert human-review responses.

Reads the reviewer JSON files and the ``*_sample.csv`` in a ``make-review`` run
folder, computes the summary statistics reported in the manuscript / reviewer
response (soundness, agent-rating adjustments, sensitivity vs. specificity, and
the systematic patterns in reviewer--agent disagreement), and renders clean,
house-style figures together with a LaTeX macro file that can be ``\\input`` into
the response document.

This complements :mod:`cli.commands.evals.human_review`'s ``review-figures``,
which produces exploratory multi-reviewer diagnostics: ``review-stats`` instead
produces the compact, quotable numbers and publication figures for a write-up.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl

from .human_review import _load_responses, _resolve_sample_csv

logger = logging.getLogger("cli")

# Likert soundness labels (line-broken for figure x-tick readability).
SOUNDNESS_LABELS: dict[int, str] = {
    1: "Not sound\nat all",
    2: "Mostly\nunsound",
    3: "Mixed or\nunsure",
    4: "Mostly\nsound",
    5: "Completely\nsound",
}

CHANGE_ORDER = ["decrease", "no_change", "increase"]
CHANGE_LABELS = {
    "decrease": "Decrease",
    "no_change": "No change",
    "increase": "Increase",
}

# Same node type on both ends is a strong proxy for an ontological is-a / sibling
# relation (e.g. ANA-ANA, CCO-CCO, BPO-BPO), which the agent tends to under-score.
SUBSUMPTION_RELATIONS: frozenset[str] = frozenset(
    ["ANA-ANA", "BPO-BPO", "CCO-CCO", "DIS-DIS", "MFN-MFN", "PHE-PHE", "PWY-PWY"]
)

_SOUND_THRESHOLD = 4  # Likert >= 4 counts as "biologically sound"
_LOW_AGENT_RATING = 2  # agent rating <= 2 == no/weak evidence

# Semantic house-style palette: blue = the entity under evaluation, grey = a
# reference/neutral series, red = a warm comparator, green = a second family.
_BLUE = "#5B9BD5"
_GREY = "#9AA0A6"
_RED = "#E0555D"
_GREEN = "#4FA06A"

# Column order for the exported per-edge human-evaluation CSV.
_EVAL_COLUMNS = [
    "reviewer",
    "edge_id",
    "seed_node_name",
    "seed_node_type",
    "target_node_name",
    "target_node_type",
    "relation_type",
    "is_true_edge",
    "agent_rating",
    "soundness",
    "rating_change",
    "abstained",
    "comment",
]


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def _load(review_dir: Path) -> pl.DataFrame:
    """Load reviewer responses joined to sampled-edge metadata for a run folder.

    Args:
        review_dir: A run folder created by ``make-review`` (contains the
            ``*_sample.csv`` and a ``responses/`` directory).

    Returns:
        One row per (reviewer, edge) judgement with soundness, rating_change,
        node/relation types, agent rating, ground-truth label, and node names.
    """
    sample = pl.read_csv(
        _resolve_sample_csv(review_dir), infer_schema_length=100000
    ).with_columns(
        pl.col("is_true_edge").cast(pl.Boolean, strict=False),
        pl.col("agent_rating").cast(pl.Int64, strict=False),
    )
    responses = _load_responses(review_dir / "responses")
    return responses.join(
        sample.select(
            "edge_id",
            "seed_node_type",
            "target_node_type",
            "relation_type",
            "agent_rating",
            "is_true_edge",
            "seed_node_name",
            "target_node_name",
        ),
        on="edge_id",
        how="left",
    )


def _pct(n: int, d: int) -> float:
    """Percentage ``n/d`` rounded to one decimal (0.0 when ``d`` is 0)."""
    return round(100 * n / d, 1) if d else 0.0


def compute_stats(df: pl.DataFrame) -> dict:
    """Compute every statistic quoted in the reviewer response.

    Args:
        df: Joined judgements from :func:`_load`.

    Returns:
        A dictionary of scalar statistics (counts, percentages, means) covering
        soundness, agent-rating adjustments, and the sensitivity/specificity
        split between positive and negative edges.
    """
    rated = df.filter(pl.col("soundness").is_not_null())
    n = df.height
    n_rated = rated.height

    sound_counts = {
        i: int(rated.filter(pl.col("soundness") == i).height) for i in range(1, 6)
    }
    n_sound = int(rated.filter(pl.col("soundness") >= _SOUND_THRESHOLD).height)

    change_counts = {
        c: int(df.filter(pl.col("rating_change") == c).height) for c in CHANGE_ORDER
    }

    # Conservatism: increases land overwhelmingly on edges the agent scored low
    # and on same-type (ontological is-a) relations.
    inc = df.filter(pl.col("rating_change") == "increase")
    inc_low_agent = int(inc.filter(pl.col("agent_rating") <= _LOW_AGENT_RATING).height)
    inc_subsumption = int(
        inc.filter(pl.col("relation_type").is_in(list(SUBSUMPTION_RELATIONS))).height
    )

    # Means from integer counts / lists (avoids float(Series.mean()), which the
    # type checker rejects because Series.mean() may be non-numeric).
    overall_mean = (
        round(sum(i * sound_counts[i] for i in range(1, 6)) / n_rated, 2)
        if n_rated
        else 0.0
    )

    def _polarity(is_true: bool) -> dict:
        """Soundness summary for positive (True) or negative (False) edges."""
        sub = rated.filter(pl.col("is_true_edge") == is_true)
        vals = [float(v) for v in sub["soundness"].drop_nulls().to_list()]
        n_ok = sum(1 for v in vals if v >= _SOUND_THRESHOLD)
        return {
            "n": sub.height,
            "mean_soundness": round(sum(vals) / len(vals), 3) if vals else None,
            "n_sound": n_ok,
            "pct_sound": _pct(n_ok, sub.height),
        }

    return {
        "n_edges": n,
        "n_reviewers": int(df["reviewer"].n_unique()),
        "n_rated": n_rated,
        "n_abstained": int(df.filter(pl.col("abstained")).height),
        "n_comments": int(df.filter(pl.col("comment") != "").height),
        "mean_soundness": overall_mean,
        "soundness_counts": sound_counts,
        "n_sound": n_sound,
        "pct_sound": _pct(n_sound, n_rated),
        "pct_completely_sound": _pct(sound_counts[5], n_rated),
        "change_counts": change_counts,
        "n_disagreements": change_counts["increase"] + change_counts["decrease"],
        "pct_no_change": _pct(change_counts["no_change"], n),
        "pct_increase": _pct(change_counts["increase"], n),
        "pct_decrease": _pct(change_counts["decrease"], n),
        "increase_to_decrease_ratio": round(
            change_counts["increase"] / max(change_counts["decrease"], 1), 1
        ),
        "n_increase_low_agent": inc_low_agent,
        "n_increase_subsumption": inc_subsumption,
        "positive": _polarity(True),
        "negative": _polarity(False),
    }


def _print_summary(s: dict) -> None:  # noqa: PLR0915
    """Log a human-readable summary of the computed statistics."""
    sep = "─" * 60
    logger.info(sep)
    logger.info("  Human review — PaperQA3 edge validation summary")
    logger.info(sep)
    logger.info(
        "  Edges reviewed        : %d  (%d reviewer)", s["n_edges"], s["n_reviewers"]
    )
    logger.info("  Abstentions           : %d", s["n_abstained"])
    logger.info("  Free-text comments    : %d", s["n_comments"])
    logger.info("")
    logger.info("  Mean soundness        : %.2f / 5", s["mean_soundness"])
    logger.info(
        "  Rated biologically sound (>= mostly sound): %d/%d  (%.1f%%)",
        s["n_sound"],
        s["n_rated"],
        s["pct_sound"],
    )
    for i in range(5, 0, -1):
        logger.info(
            "    %-18s: %d",
            SOUNDNESS_LABELS[i].replace("\n", " "),
            s["soundness_counts"][i],
        )
    logger.info("")
    logger.info("  Agent rating adjustment recommended by reviewer:")
    logger.info(
        "    No change           : %d  (%.1f%%)",
        s["change_counts"]["no_change"],
        s["pct_no_change"],
    )
    logger.info(
        "    Increase            : %d  (%.1f%%)",
        s["change_counts"]["increase"],
        s["pct_increase"],
    )
    logger.info(
        "    Decrease            : %d  (%.1f%%)",
        s["change_counts"]["decrease"],
        s["pct_decrease"],
    )
    logger.info(
        "    Increase:decrease   : %.1f : 1  (agent more conservative than human)",
        s["increase_to_decrease_ratio"],
    )
    logger.info(
        "    Of %d increases, %d were edges the agent scored <= 2 (no/weak evidence)",
        s["change_counts"]["increase"],
        s["n_increase_low_agent"],
    )
    logger.info(
        "    Of %d increases, %d were same-type (is-a / ontological) relations",
        s["change_counts"]["increase"],
        s["n_increase_subsumption"],
    )
    logger.info("")
    logger.info("  Sensitivity vs specificity:")
    logger.info(
        "    Positive edges  (n=%d): %.1f%% sound, mean %.2f",
        s["positive"]["n"],
        s["positive"]["pct_sound"],
        s["positive"]["mean_soundness"] or 0.0,
    )
    logger.info(
        "    Negative edges  (n=%d): %.1f%% sound, mean %.2f",
        s["negative"]["n"],
        s["negative"]["pct_sound"],
        s["negative"]["mean_soundness"] or 0.0,
    )
    logger.info(sep)


def _log_examples(df: pl.DataFrame, limit: int = 15) -> None:
    """Log the edges the reviewer up-rated most (agent under-scored), for context."""
    under = (
        df.filter(
            (pl.col("rating_change") == "increase")
            & (pl.col("agent_rating") <= _LOW_AGENT_RATING)
        )
        .sort("agent_rating")
        .head(limit)
    )
    if under.is_empty():
        return
    logger.info("")
    logger.info("  Edges the reviewer up-rated where the agent found no/weak evidence:")
    for r in under.iter_rows(named=True):
        subsumption = " [is-a]" if r["relation_type"] in SUBSUMPTION_RELATIONS else ""
        logger.info(
            "    (%s) %s -> %s  [agent=%d, soundness=%d]%s",
            r["relation_type"],
            r["seed_node_name"],
            r["target_node_name"],
            r["agent_rating"],
            r["soundness"],
            subsumption,
        )


def _write_macros(s: dict, path: Path) -> None:
    """Write LaTeX ``\\newcommand`` macros so the response document can ``\\input`` them."""
    macros = {
        "hrNedges": s["n_edges"],
        "hrPctSound": s["pct_sound"],
        "hrMeanSound": s["mean_soundness"],
        "hrPctNoChange": s["pct_no_change"],
        "hrPctIncrease": s["pct_increase"],
        "hrPctDecrease": s["pct_decrease"],
        "hrNincrease": s["change_counts"]["increase"],
        "hrNdecrease": s["change_counts"]["decrease"],
        "hrNdisagree": s["n_disagreements"],
        "hrIncDecRatio": s["increase_to_decrease_ratio"],
        "hrNincSubsumption": s["n_increase_subsumption"],
        "hrPosPctSound": s["positive"]["pct_sound"],
        "hrNegPctSound": s["negative"]["pct_sound"],
    }
    lines = [f"\\newcommand{{\\{k}}}{{{v}}}" for k, v in macros.items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Saved LaTeX macros to %s", path)


# --------------------------------------------------------------------------- #
# Figures (house style, applied inline so the command is self-contained)
# --------------------------------------------------------------------------- #
def _use_house_style() -> None:
    """Apply the lab's house matplotlib rcParams inline (no external style file)."""
    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"],
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
            "axes.titleweight": "normal",
            "axes.labelweight": "normal",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.axisbelow": True,
            "legend.frameon": False,
            "axes.titlesize": 12,
            "axes.labelsize": 12,
            "xtick.labelsize": 10,
            "ytick.labelsize": 10,
            "legend.fontsize": 9,
        }
    )


def _save_figure(fig: plt.Figure, stem: Path) -> None:
    """Save ``fig`` as a 600-dpi PNG and a vector PDF, tight-cropped."""
    stem.parent.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "pdf"):
        fig.savefig(stem.with_suffix(f".{ext}"), bbox_inches="tight", dpi=600)
    logger.info("Saved figure %s.{png,pdf}", stem)


def _bar_labels(ax: plt.Axes, bars, values, fmt: str = "{:d}") -> None:
    """Annotate each bar with its value, just above the bar."""
    top = max(values) if values else 0
    for b, v in zip(bars, values):
        ax.text(
            b.get_x() + b.get_width() / 2,
            b.get_height() + top * 0.015,
            fmt.format(v),
            ha="center",
            va="bottom",
            fontsize=9,
        )


def _grid_y(ax: plt.Axes) -> None:
    """Apply the house dashed, light y-grid behind the data."""
    ax.grid(axis="y", linestyle="--", alpha=0.35, zorder=0)
    ax.set_axisbelow(True)


def _draw_soundness(ax: plt.Axes, s: dict) -> None:
    """Draw the soundness-distribution bar panel."""
    xs = list(range(1, 6))
    ys = [s["soundness_counts"][i] for i in xs]
    bars = ax.bar(xs, ys, color=_BLUE, edgecolor="black", linewidth=0.6, zorder=2)
    _bar_labels(ax, bars, ys)
    ax.set_xticks(xs)
    ax.set_xticklabels([SOUNDNESS_LABELS[i] for i in xs], fontsize=9)
    ax.set_ylabel("Number of edges")
    ax.set_ylim(0, max(ys) * 1.15)
    _grid_y(ax)


def _draw_rating_change(ax: plt.Axes, s: dict) -> None:
    """Draw the agent-rating adjustment bar panel."""
    xs = list(range(3))
    ys = [s["change_counts"][c] for c in CHANGE_ORDER]
    colors = [_RED, _GREY, _GREEN]  # decrease / no change / increase
    bars = ax.bar(xs, ys, color=colors, edgecolor="black", linewidth=0.6, zorder=2)
    _bar_labels(ax, bars, ys)
    ax.set_xticks(xs)
    ax.set_xticklabels([CHANGE_LABELS[c] for c in CHANGE_ORDER])
    ax.set_ylabel("Number of edges")
    ax.set_ylim(0, max(ys) * 1.15)
    _grid_y(ax)


def _draw_polarity(ax: plt.Axes, s: dict) -> None:
    """Draw the percentage of judgements rated sound, for positive vs negative edges."""
    labels = [
        f"Positive edges\n(n={s['positive']['n']})",
        f"Negative edges\n(n={s['negative']['n']})",
    ]
    ys = [s["positive"]["pct_sound"], s["negative"]["pct_sound"]]
    bars = ax.bar(
        [0, 1],
        ys,
        color=[_BLUE, _GREY],
        edgecolor="black",
        linewidth=0.6,
        zorder=2,
        width=0.6,
    )
    _bar_labels(ax, bars, ys, fmt="{:.0f}%")
    ax.set_xticks([0, 1])
    ax.set_xticklabels(labels)
    ax.set_ylabel("Judgments rated sound (%)")
    ax.set_ylim(0, 105)
    _grid_y(ax)


def _make_figures(s: dict, fig_dir: Path) -> None:
    """Render all publication figures in the house style."""
    _use_house_style()
    fig_dir.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(6.0, 4.2))
    _draw_soundness(ax, s)
    _save_figure(fig, fig_dir / "human_review_soundness")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(5.2, 4.2))
    _draw_rating_change(ax, s)
    _save_figure(fig, fig_dir / "human_review_rating_change")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(4.6, 4.2))
    _draw_polarity(ax, s)
    _save_figure(fig, fig_dir / "human_review_by_polarity")
    plt.close(fig)

    # Combined two-panel headline figure for direct inclusion.
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.3))
    _draw_soundness(axes[0], s)
    _draw_rating_change(axes[1], s)
    axes[0].set_title("Is the agent's reasoning sound?")
    axes[1].set_title("Should the agent's rating change?")
    fig.tight_layout(w_pad=3.0)
    _save_figure(fig, fig_dir / "human_review_main")
    plt.close(fig)


def _write_evaluation_csv(df: pl.DataFrame, path: Path) -> None:
    """Write the per-edge human evaluation (one row per reviewer x edge) to CSV.

    Each row pairs a reviewer's judgement (soundness, recommended rating change,
    abstention, free-text comment) with the edge's metadata and the agent's own
    evidence rating, so the raw evaluation can be inspected or re-analysed.

    Args:
        df: Joined judgements from :func:`_load`.
        path: Destination CSV path.
    """
    cols = [c for c in _EVAL_COLUMNS if c in df.columns]
    df.select(cols).sort("reviewer", "seed_node_type", "edge_id").write_csv(path)
    logger.info("Saved human evaluation to %s", path)


def run_review_stats(review_dir: Path, out_dir: Path | None = None) -> None:
    """Compute rebuttal statistics and figures from a make-review run folder.

    Reads the reviewer JSON files from ``<review_dir>/responses`` and the sample
    CSV from ``<review_dir>``, then writes into ``out_dir`` (the run folder by
    default):

    * ``human_review_evaluation.csv`` — the raw per-edge human evaluation
      (one row per reviewer x edge, joined to edge metadata + agent rating).
    * ``rebuttal_stats.json`` — every computed statistic.
    * ``rebuttal_stats.tex`` — ``\\newcommand`` macros for the response document.
    * ``figures/human_review_{soundness,rating_change,by_polarity,main}.{png,pdf}``.

    Args:
        review_dir: A run folder created by ``make-review``.
        out_dir: Directory for outputs. Defaults to ``review_dir``.
    """
    if not review_dir.exists():
        raise FileNotFoundError(f"Review folder not found: {review_dir}")
    out_dir = out_dir or review_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _load(review_dir)
    stats = compute_stats(df)

    _print_summary(stats)
    _log_examples(df)

    _write_evaluation_csv(df, out_dir / "human_review_evaluation.csv")
    (out_dir / "rebuttal_stats.json").write_text(
        json.dumps(stats, indent=2), encoding="utf-8"
    )
    logger.info("Saved statistics to %s", out_dir / "rebuttal_stats.json")
    _write_macros(stats, out_dir / "rebuttal_stats.tex")
    _make_figures(stats, out_dir / "figures")
