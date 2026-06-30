"""Human expert validation of PaperQA3 edge evaluations.

This module supports a two-phase manual-validation workflow on top of the
PaperQA3 results produced by ``cli evals paperqa --action poll``:

1. ``make-review`` — draw a reproducible, stratified-and-balanced sample of
   edges from a polled-edges CSV and render a single self-contained HTML file.
   The HTML asks each expert reviewer to judge, on a 5-point Likert scale,
   whether the *agent's reasoning* about each edge's validity is sound. The
   file embeds all edge data, requires no server, and is meant to be shared
   over Slack. Reviewers fill it in and click "Download responses" to export a
   JSON file they send back.

2. ``review-figures`` — read every reviewer's returned JSON response file from
   a folder, join them against the sampled-edges metadata, and produce summary
   figures (soundness distribution, agreement with the agent's own rating,
   true- vs. false-edge breakdown, inter-reviewer agreement) plus an aggregated
   long-format CSV.

The ground-truth ``is_true_edge`` label is deliberately *not* embedded in the
HTML so reviewers are blind to it; it is retained in the sample CSV so it can
be compared against the human judgements during figure generation.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import polars as pl

logger = logging.getLogger("cli")

# Human-readable labels for compact node-type codes (mirrors paperqa_figures).
NODE_TYPE_LABELS: dict[str, str] = {
    "ANA": "Anatomy",
    "BPO": "Biological process",
    "CCO": "Cellular component",
    "DIS": "Disease",
    "DRG": "Drug",
    "EXP": "Exposure",
    "GEN": "Gene",
    "MFN": "Molecular function",
    "PHE": "Phenotype",
    "PWY": "Pathway",
}

# Agent evidence-rating labels (1-5), matching paperqa_figures._RATING_LABELS.
AGENT_RATING_LABELS: dict[int, str] = {
    1: "No evidence",
    2: "Weak",
    3: "Moderate",
    4: "Strong",
    5: "Very strong",
}

# Human soundness Likert scale (1-5). Reviewers judge whether the agent's
# reasoning about the edge is sound.
SOUNDNESS_LABELS: dict[int, str] = {
    1: "Not sound at all",
    2: "Mostly unsound",
    3: "Mixed / unsure",
    4: "Mostly sound",
    5: "Completely sound",
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

# Minimum reviewers per edge required to compute inter-reviewer agreement.
_MIN_REVIEWERS_FOR_AGREEMENT = 2

# Columns the HTML needs (reviewer-facing). is_true_edge is intentionally absent.
_VISIBLE_COLUMNS = [
    "edge_id",
    "seed_node_id",
    "seed_node_type",
    "seed_node_name",
    "target_node_id",
    "target_node_type",
    "target_node_name",
    "relation_type",
    "agent_rating",
    "reasoning",
]


# --------------------------------------------------------------------------- #
# Sampling
# --------------------------------------------------------------------------- #
def _load_polled(input_path: Path) -> pl.DataFrame:
    """Load a polled-edges CSV and keep only rows with a usable agent judgement.

    Args:
        input_path: Path to the polled-edges CSV produced by
            ``cli evals paperqa --action poll``.

    Returns:
        A DataFrame with a stable ``edge_id`` column and a normalised integer
        ``agent_rating`` column, filtered to rows that have non-null reasoning
        and a valid rating.

    Raises:
        FileNotFoundError: If ``input_path`` does not exist.
        ValueError: If required columns are missing.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pl.read_csv(input_path, infer_schema_length=100000)

    required = {
        "seed_node_id",
        "seed_node_type",
        "seed_node_name",
        "target_node_id",
        "target_node_type",
        "target_node_name",
        "is_true_edge",
        "reasoning",
        "rating",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input CSV is missing required columns: {missing}")

    df = df.with_columns(
        pl.col("rating").cast(pl.Int32, strict=False).alias("agent_rating"),
        pl.col("is_true_edge").cast(pl.Boolean, strict=False),
    ).filter(
        pl.col("reasoning").is_not_null()
        & (pl.col("reasoning") != "")
        & pl.col("agent_rating").is_not_null()
    )

    # Stable per-edge identifier used to join responses back to the sample.
    # Deliberately excludes is_true_edge so the ground-truth label cannot be
    # inferred from the id embedded in the (reviewer-facing) HTML. The triple
    # is unique because false edges are drawn from non-neighbours, so a given
    # (seed, target) pair never appears as both a true and a false edge.
    df = df.with_columns(
        pl.concat_str(
            [
                pl.col("seed_node_id"),
                pl.col("relation_type").fill_null("NA"),
                pl.col("target_node_id"),
            ],
            separator="__",
        ).alias("edge_id")
    )

    return df


def _stratified_balanced_sample(df: pl.DataFrame, n: int, seed: int) -> pl.DataFrame:
    """Sample ``n`` edges, balanced across true/false and stratified by node type.

    The target is a 50/50 split between true and false edges. Within each half,
    the per-node-type quota is divided as evenly as possible across the seed
    node types present, with any remainder distributed to the most populous
    strata. Strata that cannot fill their quota donate the shortfall back so the
    total still reaches ``n`` when enough edges exist.

    Args:
        df: Polled-edges DataFrame from :func:`_load_polled`.
        n: Total number of edges to sample.
        seed: Random seed for reproducibility.

    Returns:
        The sampled subset of ``df`` (at most ``n`` rows), shuffled with ``seed``.
    """
    half = n // 2
    targets = {True: half, False: n - half}
    picked: list[pl.DataFrame] = []

    for is_true, target in targets.items():
        pool = df.filter(pl.col("is_true_edge") == is_true)
        if pool.is_empty():
            continue

        node_types = sorted(pool["seed_node_type"].unique().drop_nulls().to_list())
        # Order strata by descending availability so remainder + shortfall
        # redistribution favours types that can actually supply edges.
        avail = {
            nt: pool.filter(pl.col("seed_node_type") == nt).height for nt in node_types
        }
        ordered = sorted(node_types, key=lambda nt: avail[nt], reverse=True)

        base, rem = divmod(target, len(ordered))
        quota = {nt: base + (1 if i < rem else 0) for i, nt in enumerate(ordered)}

        # First pass: take min(quota, available); track shortfall.
        chosen: list[pl.DataFrame] = []
        taken: dict[str, int] = {}
        for nt in ordered:
            k = min(quota[nt], avail[nt])
            taken[nt] = k
            if k > 0:
                chosen.append(
                    pool.filter(pl.col("seed_node_type") == nt).sample(n=k, seed=seed)
                )

        # Second pass: redistribute any shortfall to strata with spare capacity.
        shortfall = target - sum(taken.values())
        if shortfall > 0:
            for nt in ordered:
                if shortfall <= 0:
                    break
                spare = avail[nt] - taken[nt]
                if spare <= 0:
                    continue
                extra = min(spare, shortfall)
                already = (
                    pool.filter(pl.col("seed_node_type") == nt)
                    .sample(n=taken[nt] + extra, seed=seed)
                    .tail(extra)
                )
                chosen.append(already)
                taken[nt] += extra
                shortfall -= extra

        if chosen:
            picked.append(pl.concat(chosen))

    if not picked:
        raise ValueError("No edges available to sample. Check the input CSV.")

    result = pl.concat(picked)
    # Shuffle so reviewers don't see all true edges grouped together.
    return result.sample(fraction=1.0, shuffle=True, seed=seed)


# --------------------------------------------------------------------------- #
# HTML rendering
# --------------------------------------------------------------------------- #
def _build_html(sample: pl.DataFrame, seed: int, n: int) -> str:
    """Render the self-contained reviewer HTML for a sampled set of edges.

    Args:
        sample: Sampled edges (must contain the columns in ``_VISIBLE_COLUMNS``).
        seed: Random seed used to draw the sample (recorded in the export).
        n: Number of edges in the sample (recorded in the export).

    Returns:
        A complete HTML document as a string.
    """
    visible = sample.select(_VISIBLE_COLUMNS)
    # Decorate each record with display-friendly labels computed in Python.
    records = []
    for row in visible.iter_rows(named=True):
        rec = dict(row)
        rating = int(rec["agent_rating"])
        rec["agent_rating_label"] = AGENT_RATING_LABELS.get(rating, str(rating))
        rec["seed_type_label"] = NODE_TYPE_LABELS.get(
            rec["seed_node_type"], rec["seed_node_type"]
        )
        rec["target_type_label"] = NODE_TYPE_LABELS.get(
            rec["target_node_type"], rec["target_node_type"]
        )
        records.append(rec)

    data_json = json.dumps(records, ensure_ascii=False)
    soundness_json = json.dumps(SOUNDNESS_LABELS, ensure_ascii=False)

    return (
        _HTML_TEMPLATE.replace("__DATA_JSON__", data_json)
        .replace("__SOUNDNESS_JSON__", soundness_json)
        .replace("__SEED__", str(seed))
        .replace("__N__", str(n))
    )


def run_make_review(
    input_path: Path,
    out_dir: Path,
    n: int = 100,
    seed: int = 42,
) -> None:
    """Generate the reviewer HTML and the sample CSV for a polled-edges file.

    Writes two files into ``out_dir``:

    * ``human_review_seed=<seed>_n=<n>.html`` — the self-contained form to
      share with reviewers over Slack.
    * ``human_review_seed=<seed>_n=<n>_sample.csv`` — the sampled edges,
      including the hidden ``is_true_edge`` ground truth, for later joining.

    Args:
        input_path: Path to the polled-edges CSV.
        out_dir: Directory to write outputs into.
        n: Number of edges to sample.
        seed: Random seed for reproducibility.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _load_polled(input_path)
    logger.info("Loaded %d usable polled edges from %s", df.height, input_path)

    if df.height < n:
        logger.warning(
            "Only %d usable edges available; sampling all of them (requested %d).",
            df.height,
            n,
        )
        n = df.height

    sample = _stratified_balanced_sample(df, n=n, seed=seed)
    logger.info("Sampled %d edges (seed=%d).", sample.height, seed)

    stem = f"human_review_seed={seed}_n={sample.height}"

    # Sample CSV retains ground truth + all columns for the figures step.
    sample_path = out_dir / f"{stem}_sample.csv"
    sample.sort("seed_node_type", "is_true_edge").write_csv(sample_path)
    logger.info("Saved sample edges to %s", sample_path)

    html = _build_html(sample, seed=seed, n=sample.height)
    html_path = out_dir / f"{stem}.html"
    html_path.write_text(html, encoding="utf-8")
    logger.info("Saved reviewer HTML to %s", html_path)

    # Report the realised stratification so the operator can sanity-check it.
    breakdown = (
        sample.group_by("seed_node_type", "is_true_edge")
        .len()
        .sort("seed_node_type", "is_true_edge")
    )
    n_true = sample.filter(pl.col("is_true_edge")).height
    logger.info(
        "Sample composition: %d true / %d false edges across %d node types.",
        n_true,
        sample.height - n_true,
        sample["seed_node_type"].n_unique(),
    )
    for r in breakdown.iter_rows(named=True):
        logger.info(
            "  %-4s %-5s : %d",
            r["seed_node_type"],
            "true" if r["is_true_edge"] else "false",
            r["len"],
        )
    logger.info(
        "Share the HTML with reviewers. Ask them to deposit their downloaded "
        "JSON responses into a folder, then run `cli evals review-figures`."
    )


# --------------------------------------------------------------------------- #
# Figures from returned responses
# --------------------------------------------------------------------------- #
def _load_responses(responses_dir: Path) -> pl.DataFrame:
    """Read all reviewer response JSON files in a directory into long format.

    Args:
        responses_dir: Directory containing ``*.json`` files exported from the
            reviewer HTML.

    Returns:
        A long-format DataFrame with one row per (reviewer, edge) judgement and
        columns ``reviewer``, ``edge_id``, ``soundness`` (Int, null if abstained
        or unanswered), ``abstained`` (Bool), and ``comment``.

    Raises:
        FileNotFoundError: If the directory does not exist.
        ValueError: If no parseable response files are found.
    """
    if not responses_dir.exists():
        raise FileNotFoundError(f"Responses directory not found: {responses_dir}")

    rows: list[dict] = []
    files = sorted(responses_dir.glob("*.json"))
    for fp in files:
        try:
            payload = json.loads(fp.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Skipping unreadable response file %s: %s", fp, exc)
            continue

        reviewer = (payload.get("reviewer_name") or fp.stem).strip() or fp.stem
        for resp in payload.get("responses", []):
            edge_id = resp.get("edge_id")
            if edge_id is None:
                continue
            abstained = bool(resp.get("abstained", False))
            soundness = resp.get("soundness")
            rows.append(
                {
                    "reviewer": reviewer,
                    "edge_id": edge_id,
                    "soundness": None
                    if abstained or soundness is None
                    else int(soundness),
                    "abstained": abstained,
                    "comment": (resp.get("comment") or "").strip(),
                }
            )

    if not rows:
        raise ValueError(
            f"No parseable reviewer judgements found in {responses_dir}. "
            "Expected JSON files exported from the reviewer HTML."
        )

    logger.info("Loaded %d judgements from %d response file(s).", len(rows), len(files))
    return pl.DataFrame(rows)


def _join_responses(responses: pl.DataFrame, sample: pl.DataFrame) -> pl.DataFrame:
    """Join reviewer judgements against the sampled-edge metadata.

    Args:
        responses: Long-format judgements from :func:`_load_responses`.
        sample: The sample CSV written by :func:`run_make_review`.

    Returns:
        ``responses`` left-joined to per-edge metadata (node types, relation
        type, agent rating, ground-truth ``is_true_edge``).
    """
    meta = sample.select(
        "edge_id",
        "seed_node_type",
        "relation_type",
        "agent_rating",
        "is_true_edge",
    )
    joined = responses.join(meta, on="edge_id", how="left")
    unmatched = joined.filter(pl.col("seed_node_type").is_null()).height
    if unmatched:
        logger.warning(
            "%d judgement(s) did not match any edge in the sample CSV "
            "(stale or mismatched responses?).",
            unmatched,
        )
    return joined.filter(pl.col("seed_node_type").is_not_null())


def _ax_style(ax: plt.Axes) -> None:
    """Apply the shared minimal axis styling used across eval figures."""
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(axis="both", which="both", top=False, right=False, labelsize=8)


def _plot_review_figures(joined: pl.DataFrame, out_path: Path) -> None:  # noqa: PLR0915
    """Render the four-panel human-review summary figure.

    Args:
        joined: Joined judgements from :func:`_join_responses`.
        out_path: Destination PDF path (an SVG sibling is also written).
    """
    rated = joined.filter(pl.col("soundness").is_not_null())
    ratings = [1, 2, 3, 4, 5]
    rating_labels = [SOUNDNESS_LABELS[r] for r in ratings]

    fig, axes = plt.subplots(2, 2, figsize=(13, 10))

    # Panel 1: overall soundness distribution, stacked by reviewer.
    ax = axes[0][0]
    reviewers = sorted(rated["reviewer"].unique().to_list())
    bottoms = [0] * len(ratings)
    for i, rev in enumerate(reviewers):
        sub = rated.filter(pl.col("reviewer") == rev)
        heights = [sub.filter(pl.col("soundness") == r).height for r in ratings]
        ax.bar(
            ratings,
            heights,
            bottom=bottoms,
            color=_PALETTE[i % len(_PALETTE)],
            edgecolor="black",
            linewidth=0.5,
            width=0.7,
            label=rev,
        )
        bottoms = [b + h for b, h in zip(bottoms, heights)]
    ax.set_title("Soundness of agent reasoning (all reviewers)", fontsize=12)
    ax.set_xticks(ratings)
    ax.set_xticklabels(rating_labels, fontsize=7, rotation=20, ha="right")
    ax.set_ylabel("Judgements", fontsize=10)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.legend(title="Reviewer", fontsize=7, title_fontsize=8, frameon=False)
    _ax_style(ax)

    # Panel 2: human soundness vs agent evidence rating (mean +/- spread).
    ax = axes[0][1]
    agent_ratings = [1, 2, 3, 4, 5]
    means, counts = [], []
    for ar in agent_ratings:
        sub = rated.filter(pl.col("agent_rating") == ar)
        means.append(sub["soundness"].mean() if sub.height else None)
        counts.append(sub.height)
    xs = [a for a, m in zip(agent_ratings, means) if m is not None]
    ys = [m for m in means if m is not None]
    ax.bar(
        xs,
        ys,
        color=_PALETTE[2],
        edgecolor="black",
        linewidth=0.5,
        width=0.7,
    )
    for a, m, c in zip(agent_ratings, means, counts):
        if m is not None:
            ax.text(a, m + 0.05, f"n={c}", ha="center", va="bottom", fontsize=8)
    ax.set_title("Mean human soundness vs. agent evidence rating", fontsize=12)
    ax.set_xticks(agent_ratings)
    ax.set_xticklabels(
        [AGENT_RATING_LABELS[a] for a in agent_ratings],
        fontsize=7,
        rotation=20,
        ha="right",
    )
    ax.set_ylabel("Mean soundness (1-5)", fontsize=10)
    ax.set_ylim(0, 5.5)
    _ax_style(ax)

    # Panel 3: soundness distribution split by ground-truth edge validity.
    ax = axes[1][0]
    bar_w = 0.38
    for offset, is_true, color, lbl in [
        (-bar_w / 2, True, _PALETTE[0], "True edges"),
        (bar_w / 2, False, _PALETTE[7], "False edges"),
    ]:
        sub = rated.filter(pl.col("is_true_edge") == is_true)
        total = max(sub.height, 1)
        heights = [sub.filter(pl.col("soundness") == r).height / total for r in ratings]
        ax.bar(
            [r + offset for r in ratings],
            heights,
            width=bar_w,
            color=color,
            edgecolor="black",
            linewidth=0.5,
            label=f"{lbl} (n={sub.height})",
        )
    ax.set_title("Soundness by ground-truth edge validity", fontsize=12)
    ax.set_xticks(ratings)
    ax.set_xticklabels(rating_labels, fontsize=7, rotation=20, ha="right")
    ax.set_ylabel("Proportion of judgements", fontsize=10)
    ax.legend(fontsize=8, frameon=False)
    _ax_style(ax)

    # Panel 4: inter-reviewer agreement -- per-edge soundness standard deviation.
    ax = axes[1][1]
    per_edge = (
        rated.group_by("edge_id")
        .agg(
            pl.col("soundness").std().alias("sd"),
            pl.len().alias("n_reviewers"),
        )
        .filter(pl.col("n_reviewers") >= _MIN_REVIEWERS_FOR_AGREEMENT)
    )
    if per_edge.height:
        sds = per_edge["sd"].fill_null(0.0).to_numpy()
        ax.hist(
            sds,
            bins=[0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0],
            color=_PALETTE[8],
            edgecolor="black",
            linewidth=0.5,
        )
        ax.axvline(
            float(sds.mean()),
            color=_PALETTE[7],
            linestyle="--",
            linewidth=1.2,
            label=f"mean SD = {sds.mean():.2f}",
        )
        ax.legend(fontsize=8, frameon=False)
        ax.set_title(
            f"Inter-reviewer agreement ({per_edge.height} multiply-rated edges)",
            fontsize=12,
        )
        ax.set_xlabel("Per-edge soundness standard deviation", fontsize=10)
        ax.set_ylabel("Edges", fontsize=10)
    else:
        ax.set_title("Inter-reviewer agreement (needs >=2 reviewers/edge)", fontsize=12)
        ax.text(0.5, 0.5, "Not enough overlap", ha="center", va="center")
        ax.set_xticks([])
        ax.set_yticks([])
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    _ax_style(ax)

    fig.tight_layout(pad=2.0)
    fig.savefig(out_path, bbox_inches="tight")
    fig.savefig(out_path.with_suffix(".svg"), bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved human-review figure to %s", out_path)


def _print_review_stats(joined: pl.DataFrame) -> None:
    """Log a console summary of the aggregated human-review judgements."""
    sep = "─" * 56
    rated = joined.filter(pl.col("soundness").is_not_null())
    logger.info(sep)
    logger.info("  Human review summary")
    logger.info(sep)
    logger.info("  Reviewers        : %d", joined["reviewer"].n_unique())
    logger.info("  Total judgements : %d", joined.height)
    logger.info("  Abstentions      : %d", joined.filter(pl.col("abstained")).height)
    logger.info("  Scored judgements: %d", rated.height)
    if rated.height:
        logger.info("  Mean soundness   : %.2f", rated["soundness"].mean())

    logger.info("")
    logger.info("  Mean soundness by ground-truth validity:")
    for is_true, lbl in [(True, "True edges "), (False, "False edges")]:
        sub = rated.filter(pl.col("is_true_edge") == is_true)
        if sub.height:
            logger.info(
                "    %s : %.2f (n=%d)", lbl, sub["soundness"].mean(), sub.height
            )

    logger.info("")
    logger.info("  Mean soundness by reviewer:")
    by_rev = (
        rated.group_by("reviewer")
        .agg(pl.col("soundness").mean().alias("mean"), pl.len().alias("n"))
        .sort("reviewer")
    )
    for r in by_rev.iter_rows(named=True):
        logger.info("    %-20s : %.2f (n=%d)", r["reviewer"], r["mean"], r["n"])
    logger.info(sep)


def run_review_figures(
    responses_dir: Path,
    sample_path: Path,
    out_dir: Path | None = None,
) -> None:
    """Aggregate reviewer responses and generate human-review figures.

    Args:
        responses_dir: Directory of reviewer-exported JSON response files.
        sample_path: Path to the ``*_sample.csv`` written by ``make-review``.
        out_dir: Directory for outputs. Defaults to ``responses_dir``.
    """
    out_dir = out_dir or responses_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if not sample_path.exists():
        raise FileNotFoundError(f"Sample CSV not found: {sample_path}")

    sample = pl.read_csv(sample_path, infer_schema_length=100000).with_columns(
        pl.col("is_true_edge").cast(pl.Boolean, strict=False),
        pl.col("agent_rating").cast(pl.Int32, strict=False),
    )
    responses = _load_responses(responses_dir)
    joined = _join_responses(responses, sample)

    aggregated_path = out_dir / "human_review_responses_long.csv"
    joined.sort("reviewer", "edge_id").write_csv(aggregated_path)
    logger.info("Saved aggregated responses to %s", aggregated_path)

    _print_review_stats(joined)
    _plot_review_figures(joined, out_dir / "human_review_figures.pdf")


# --------------------------------------------------------------------------- #
# HTML template (no external dependencies; data injected via .replace()).
# --------------------------------------------------------------------------- #
_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OptimusKG &mdash; Expert Edge Review</title>
<style>
  :root {
    --ink: #26251e; --muted: #57534e; --line: #e7e5e4; --bg: #faf9f7;
    --card: #ffffff; --accent: #516FD9; --accent-soft: #eef2fd;
    --true: #6FA430; --warn: #DA3546;
  }
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    color: var(--ink); background: var(--bg); margin: 0; line-height: 1.5;
  }
  header {
    position: sticky; top: 0; z-index: 10; background: var(--card);
    border-bottom: 1px solid var(--line); padding: 16px 24px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
  }
  header h1 { margin: 0 0 4px; font-size: 18px; }
  header p { margin: 0; color: var(--muted); font-size: 13px; }
  .bar-wrap { margin-top: 10px; height: 8px; background: var(--line); border-radius: 4px; overflow: hidden; }
  .bar-fill { height: 100%; width: 0%; background: var(--accent); transition: width 0.2s; }
  .controls { display: flex; flex-wrap: wrap; gap: 12px; align-items: end; margin-top: 12px; }
  .controls label { font-size: 12px; color: var(--muted); display: block; margin-bottom: 3px; }
  .controls input {
    font-size: 14px; padding: 7px 10px; border: 1px solid var(--line);
    border-radius: 6px; min-width: 200px;
  }
  button {
    font-size: 14px; font-weight: 600; padding: 9px 16px; border: none;
    border-radius: 6px; background: var(--accent); color: #fff; cursor: pointer;
  }
  button:disabled { background: #b8c2e8; cursor: not-allowed; }
  main { max-width: 920px; margin: 0 auto; padding: 24px 16px 120px; }
  .card {
    background: var(--card); border: 1px solid var(--line); border-radius: 10px;
    padding: 20px; margin-bottom: 20px; box-shadow: 0 1px 2px rgba(0,0,0,0.03);
  }
  .card.answered { border-left: 4px solid var(--true); }
  .edge-index { font-size: 12px; color: var(--muted); font-weight: 600; }
  .edge-stmt { font-size: 17px; margin: 8px 0 4px; }
  .pill {
    display: inline-block; background: var(--accent-soft); color: var(--accent);
    padding: 2px 9px; border-radius: 99px; font-size: 12px; font-weight: 600;
  }
  .node-name { font-weight: 700; }
  .node-type { color: var(--muted); font-size: 13px; }
  .agent-box {
    background: var(--bg); border: 1px solid var(--line); border-radius: 8px;
    padding: 14px 16px; margin: 14px 0; font-size: 14px;
  }
  .agent-box .lbl { font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; color: var(--muted); font-weight: 700; }
  .agent-rating { font-weight: 700; color: var(--accent); }
  .reasoning { margin-top: 8px; white-space: pre-wrap; }
  .q { font-weight: 600; margin: 18px 0 10px; }
  .likert { display: flex; flex-wrap: wrap; gap: 8px; }
  .likert label {
    flex: 1; min-width: 120px; border: 1px solid var(--line); border-radius: 8px;
    padding: 10px; text-align: center; cursor: pointer; font-size: 13px; background: var(--card);
  }
  .likert label:hover { border-color: var(--accent); }
  .likert input { display: none; }
  .likert input:checked + span { font-weight: 700; }
  .likert label:has(input:checked) { border-color: var(--accent); background: var(--accent-soft); }
  .likert .num { display: block; font-size: 18px; font-weight: 700; color: var(--accent); }
  .abstain { margin-top: 12px; font-size: 13px; color: var(--muted); }
  .abstain input { margin-right: 6px; }
  textarea {
    width: 100%; margin-top: 10px; border: 1px solid var(--line); border-radius: 8px;
    padding: 10px; font-family: inherit; font-size: 13px; resize: vertical; min-height: 52px;
  }
  footer {
    position: fixed; bottom: 0; left: 0; right: 0; background: var(--card);
    border-top: 1px solid var(--line); padding: 12px 24px; display: flex;
    align-items: center; justify-content: space-between; gap: 16px;
  }
  footer .status { font-size: 13px; color: var(--muted); }
  .note { font-size: 12px; color: var(--muted); }
</style>
</head>
<body>
<header>
  <h1>OptimusKG &mdash; Expert Edge Review</h1>
  <p>For each edge below, the AI agent gathered literature evidence and reasoned about whether the
     relationship is valid. <strong>Your task:</strong> judge whether the agent&rsquo;s reasoning is sound.</p>
  <div class="controls">
    <div>
      <label for="rev-name">Your name (required)</label>
      <input id="rev-name" type="text" placeholder="Jane Doe" autocomplete="name">
    </div>
    <div>
      <label for="rev-email">Email (optional)</label>
      <input id="rev-email" type="email" placeholder="jane@institution.edu" autocomplete="email">
    </div>
  </div>
  <div class="bar-wrap"><div id="bar" class="bar-fill"></div></div>
  <p class="note" id="progress-text" style="margin-top:6px"></p>
</header>
<main id="cards"></main>
<footer>
  <span class="status" id="footer-status">Your progress is saved automatically in this browser.</span>
  <button id="download-btn" disabled>Download responses</button>
</footer>
<script id="edge-data" type="application/json">__DATA_JSON__</script>
<script id="soundness-labels" type="application/json">__SOUNDNESS_JSON__</script>
<script>
(function () {
  "use strict";
  var SEED = __SEED__, N = __N__;
  var EDGES = JSON.parse(document.getElementById("edge-data").textContent);
  var SOUNDNESS = JSON.parse(document.getElementById("soundness-labels").textContent);
  var STORAGE_KEY = "optimuskg_human_review_seed" + SEED + "_n" + N;
  var state = loadState();

  function loadState() {
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY)) || {}; }
    catch (e) { return {}; }
  }
  function saveState() {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(state)); } catch (e) {}
  }

  var nameInput = document.getElementById("rev-name");
  var emailInput = document.getElementById("rev-email");
  nameInput.value = state.__name || "";
  emailInput.value = state.__email || "";
  nameInput.addEventListener("input", function () { state.__name = nameInput.value; saveState(); refresh(); });
  emailInput.addEventListener("input", function () { state.__email = emailInput.value; saveState(); });

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  var container = document.getElementById("cards");
  EDGES.forEach(function (edge, i) {
    var saved = state[edge.edge_id] || {};
    var card = document.createElement("section");
    card.className = "card";
    card.id = "card-" + i;

    var likert = "";
    for (var r = 1; r <= 5; r++) {
      var checked = saved.soundness === r ? " checked" : "";
      likert +=
        '<label><input type="radio" name="s-' + i + '" value="' + r + '"' + checked + '>' +
        '<span><span class="num">' + r + '</span>' + esc(SOUNDNESS[r]) + '</span></label>';
    }

    var relation = edge.relation_type ? edge.relation_type : "related to";
    card.innerHTML =
      '<div class="edge-index">Edge ' + (i + 1) + ' of ' + EDGES.length + '</div>' +
      '<div class="edge-stmt">' +
        '<span class="node-name">' + esc(edge.seed_node_name) + '</span> ' +
        '<span class="node-type">(' + esc(edge.seed_type_label) + ')</span> ' +
        '<span class="pill">' + esc(relation) + '</span> ' +
        '<span class="node-name">' + esc(edge.target_node_name) + '</span> ' +
        '<span class="node-type">(' + esc(edge.target_type_label) + ')</span>' +
      '</div>' +
      '<div class="agent-box">' +
        '<span class="lbl">Agent evidence rating:</span> ' +
        '<span class="agent-rating">' + edge.agent_rating + '/5 &mdash; ' + esc(edge.agent_rating_label) + '</span>' +
        '<div class="reasoning">' + esc(edge.reasoning) + '</div>' +
      '</div>' +
      '<div class="q">Is the agent&rsquo;s reasoning about this edge sound?</div>' +
      '<div class="likert">' + likert + '</div>' +
      '<div class="abstain"><label><input type="checkbox" id="ab-' + i + '"' +
        (saved.abstained ? " checked" : "") + '> I don&rsquo;t have enough expertise to judge this edge</label></div>' +
      '<textarea id="c-' + i + '" placeholder="Optional comment (explain your rating or flag an issue)">' +
        esc(saved.comment || "") + '</textarea>';
    container.appendChild(card);

    card.querySelectorAll('input[name="s-' + i + '"]').forEach(function (radio) {
      radio.addEventListener("change", function () {
        record(edge.edge_id, { soundness: parseInt(radio.value, 10), abstained: false });
        document.getElementById("ab-" + i).checked = false;
      });
    });
    document.getElementById("ab-" + i).addEventListener("change", function (e) {
      if (e.target.checked) {
        card.querySelectorAll('input[name="s-' + i + '"]').forEach(function (rd) { rd.checked = false; });
        record(edge.edge_id, { soundness: null, abstained: true });
      } else {
        record(edge.edge_id, { abstained: false });
      }
    });
    document.getElementById("c-" + i).addEventListener("input", function (e) {
      record(edge.edge_id, { comment: e.target.value });
    });
  });

  function record(edgeId, patch) {
    state[edgeId] = Object.assign({}, state[edgeId], patch);
    saveState();
    refresh();
  }

  function isAnswered(s) { return s && (typeof s.soundness === "number" || s.abstained === true); }

  function refresh() {
    var done = 0;
    EDGES.forEach(function (edge, i) {
      var ans = isAnswered(state[edge.edge_id]);
      if (ans) done++;
      var card = document.getElementById("card-" + i);
      if (card) card.classList.toggle("answered", !!ans);
    });
    var pct = EDGES.length ? Math.round(done / EDGES.length * 100) : 0;
    document.getElementById("bar").style.width = pct + "%";
    document.getElementById("progress-text").textContent =
      done + " of " + EDGES.length + " edges reviewed (" + pct + "%)";
    var ready = done === EDGES.length && nameInput.value.trim().length > 0;
    document.getElementById("download-btn").disabled = !(done > 0 && nameInput.value.trim().length > 0);
    document.getElementById("footer-status").textContent = ready
      ? "All edges reviewed — ready to download and send back."
      : "Your progress is saved automatically in this browser.";
  }

  function download() {
    var name = nameInput.value.trim();
    if (!name) { alert("Please enter your name before downloading."); return; }
    var responses = EDGES.map(function (edge) {
      var s = state[edge.edge_id] || {};
      return {
        edge_id: edge.edge_id,
        soundness: typeof s.soundness === "number" ? s.soundness : null,
        abstained: s.abstained === true,
        comment: s.comment || ""
      };
    });
    var answered = responses.filter(function (r) { return r.soundness !== null || r.abstained; }).length;
    if (answered < EDGES.length &&
        !confirm("You have reviewed " + answered + " of " + EDGES.length +
                 " edges. Download anyway?")) { return; }
    var payload = {
      reviewer_name: name,
      reviewer_email: emailInput.value.trim(),
      seed: SEED, n: N,
      exported_at: new Date().toISOString(),
      responses: responses
    };
    var safe = name.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "") || "reviewer";
    var blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = "human_review_response_" + safe + ".json";
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  document.getElementById("download-btn").addEventListener("click", download);
  refresh();
})();
</script>
</body>
</html>
"""
