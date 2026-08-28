"""Power and precision analysis for the PaperQA3 edge validation.

Answers the question of whether the validated sample is large enough to support
the graph-quality claims made in the manuscript.

The analysis rests on three points:

* **Sampling fraction is irrelevant.** Precision of a proportion depends on the
  absolute sample size, not on what share of the population was sampled. The
  finite-population correction for ~4.7k edges out of ~21.8M is ~0.9999.
* **The design is clustered, and that is what costs precision.** Edges were
  sampled as up to ten neighbours of each seed node, so they are not
  independent. Intervals are therefore estimated with ``svy`` under an explicit
  survey design (seed node as PSU, node type as stratum) rather than by
  assuming independent observations.
* **The estimand is narrower than "a uniformly random edge".** Seeds were drawn
  with equal allocation per node type after excluding the top and bottom decile
  by within-type degree. The coverage table quantifies what that leaves in
  scope.

Outputs a JSON report, a logged summary, and a two-panel figure.
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal, cast

import matplotlib.pyplot as plt
import polars as pl
import svy
from scipy import stats
from statsmodels.stats.proportion import power_proportions_2indep, proportion_confint

from .utils import NODE_TYPE_LABELS, THEMES, load_polled_edges, normalize_relation_types

logger = logging.getLogger("cli")

# A rating of 1 means "no supporting evidence found"; anything above it counts
# as literature support.
RATING_NO_EVIDENCE = 1

# Total edges in the released graph, used only for the finite-population
# correction and the full-graph cost extrapolation.
GRAPH_TOTAL_EDGES = 21_834_669

# Strata below this effective sample size are reported as descriptive only.
MIN_USABLE_EFFECTIVE_N = 30.0

# On the five-point soundness scale, 4 ("mostly sound") and 5 ("completely
# sound") count as the agent's reasoning being judged biologically sound.
SOUND_THRESHOLD = 4

DEFAULT_ALPHA = 0.05

# USD per evaluated edge. Each PaperQA3 query consumes one credit, priced at $1
# at the time of submission; the reported validation consumed 5,706 credits
# (4,706 true edges plus 1,000 negative controls, excluding two failed queries).
DEFAULT_COST_PER_EDGE_USD = 1.0

# Columns used to declare the survey design to svy.
_OUTCOME = "supported"
_PSU = "seed_node_id"
_STRATUM = "seed_node_type"

# Reported outcome of the blinded 100-edge expert review. Used when no raw
# reviewer export is supplied via --human-review.
HUMAN_REVIEW_SUMMARY: dict[str, tuple[int, int]] = {
    "Reasoning sound (all edges)": (92, 100),
    "Reasoning sound (true edges)": (42, 50),
    "Reasoning sound (negative controls)": (50, 50),
    "Reviewer would not change rating": (72, 100),
    "Reviewer would increase rating": (25, 100),
    "Reviewer would decrease rating": (3, 100),
}

_SEPARATOR = "─" * 72


@dataclass
class ProportionEstimate:
    """A proportion with design-adjusted and naive uncertainty."""

    label: str
    successes: int
    n: int
    proportion: float
    standard_error: float
    ci_low: float
    ci_high: float
    naive_ci_low: float
    naive_ci_high: float
    design_effect: float
    effective_n: float
    icc: float

    @property
    def naive_half_width_pp(self) -> float:
        """Half-width of the interval that assumes independent sampling."""
        return (self.naive_ci_high - self.naive_ci_low) / 2 * 100

    @property
    def half_width_pp(self) -> float:
        """Half-width of the design-adjusted interval, in percentage points."""
        return (self.ci_high - self.ci_low) / 2 * 100

    @property
    def is_usable(self) -> bool:
        """Whether the stratum has enough effective observations to interpret."""
        return self.effective_n >= MIN_USABLE_EFFECTIVE_N


@dataclass
class CoverageRow:
    """Share of a node type left in the sampling frame after degree exclusion."""

    node_type: str
    n_nodes: int
    n_kept: int
    frac_nodes_kept: float
    frac_degree_mass_kept: float


@dataclass
class PowerReport:
    """Complete power/precision report for one validation run."""

    run_id: str
    alpha: float
    cost_per_edge_usd: float
    finite_population_correction: float
    graph_total_edges: int
    overall: list[ProportionEstimate] = field(default_factory=list)
    by_node_type: list[ProportionEstimate] = field(default_factory=list)
    by_relation_type: list[ProportionEstimate] = field(default_factory=list)
    human_review: list[ProportionEstimate] = field(default_factory=list)
    human_review_by_node_type: list[ProportionEstimate] = field(default_factory=list)
    rating_asymmetry: dict[str, float] = field(default_factory=dict)
    contrast: dict[str, float] = field(default_factory=dict)
    precision_curve: list[dict[str, float]] = field(default_factory=list)
    coverage: list[CoverageRow] = field(default_factory=list)


def finite_population_correction(n: int, population: int) -> float:
    """Standard-error multiplier for sampling without replacement.

    Args:
        n: Sample size.
        population: Population size.

    Returns:
        The correction factor ``sqrt(1 - n / N)``; ~1 when n << N.
    """
    if population <= 0 or n >= population:
        return 0.0
    return math.sqrt(1 - n / population)


def _build_estimate(  # noqa: PLR0913
    label: str,
    successes: int,
    n: int,
    proportion: float,
    standard_error: float,
    ci_low: float,
    ci_high: float,
    mean_cluster_size: float,
    alpha: float,
) -> ProportionEstimate:
    """Assemble an estimate, deriving the design effect from the survey SE.

    ``svy`` exposes a ``deff`` flag but returns NaN for it in 0.21.0, so the
    design effect is recovered by comparing the design-based standard error
    against the simple-random-sampling standard error for the same data. The
    intra-cluster correlation is then inverted from the design effect.

    Args:
        label: Human-readable name for the estimate.
        successes: Number of successes.
        n: Number of observations.
        proportion: Point estimate.
        standard_error: Design-based standard error.
        ci_low: Design-based lower bound.
        ci_high: Design-based upper bound.
        mean_cluster_size: Average observations per PSU.
        alpha: Two-sided significance level.

    Returns:
        A populated :class:`ProportionEstimate`.
    """
    naive_low, naive_high = proportion_confint(
        successes, n, alpha=alpha, method="wilson"
    )

    srs_variance = proportion * (1 - proportion) / n if n else 0.0
    deff = max(1.0, standard_error**2 / srs_variance) if srs_variance > 0 else 1.0
    icc = (deff - 1) / (mean_cluster_size - 1) if mean_cluster_size > 1 else 0.0

    return ProportionEstimate(
        label=label,
        successes=successes,
        n=n,
        proportion=proportion,
        standard_error=standard_error,
        ci_low=ci_low,
        ci_high=ci_high,
        naive_ci_low=float(naive_low),
        naive_ci_high=float(naive_high),
        design_effect=deff,
        effective_n=n / deff,
        icc=max(0.0, min(1.0, icc)),
    )


def survey_estimates(
    df: pl.DataFrame, alpha: float, by: str | None = None
) -> list[ProportionEstimate]:
    """Estimate literature support under the clustered, stratified design.

    Args:
        df: True-edge rows carrying the outcome, PSU and stratum columns.
        alpha: Two-sided significance level.
        by: Optional column producing one estimate per level.

    Returns:
        Estimates, sorted by descending sample size when grouped.
    """
    # Stratum is only meaningful for the ungrouped and by-node-type estimates;
    # slicing by relation type would leave strata with a single PSU.
    stratum = _STRATUM if by in (None, _STRATUM) else None
    sample = svy.Sample(data=df, design=svy.Design(psu=_PSU, stratum=stratum))
    # `prop` returns a list only when passed several outcome columns.
    estimate = cast(
        "svy.Estimate",
        sample.estimation.prop(_OUTCOME, by=by, alpha=alpha, ci_method="wilson"),
    )
    table = estimate.to_polars().filter(pl.col(_OUTCOME).cast(pl.String) == "1")

    estimates: list[ProportionEstimate] = []
    for row in table.iter_rows(named=True):
        subset = df if by is None else df.filter(pl.col(by) == row[by])
        estimates.append(
            _build_estimate(
                label="Literature support, true edges" if by is None else str(row[by]),
                successes=int(subset[_OUTCOME].sum()),
                n=subset.height,
                proportion=row["est"],
                standard_error=row["se"],
                ci_low=row["lci"],
                ci_high=row["uci"],
                mean_cluster_size=subset.height / subset[_PSU].n_unique(),
                alpha=alpha,
            )
        )
    return sorted(estimates, key=lambda e: e.n, reverse=True) if by else estimates


def simple_estimate(
    label: str, successes: int, n: int, alpha: float = DEFAULT_ALPHA
) -> ProportionEstimate:
    """Estimate an unclustered proportion, where no design adjustment applies.

    Used for the negative controls, of which exactly one was drawn per seed,
    and for the expert review, which carries one judgement per edge.

    Args:
        label: Human-readable name for the estimate.
        successes: Number of successes.
        n: Number of observations.
        alpha: Two-sided significance level.

    Returns:
        A populated :class:`ProportionEstimate` with design effect 1.
    """
    proportion = successes / n if n else 0.0
    low, high = proportion_confint(successes, n, alpha=alpha, method="wilson")
    return ProportionEstimate(
        label=label,
        successes=successes,
        n=n,
        proportion=proportion,
        standard_error=math.sqrt(proportion * (1 - proportion) / n) if n else 0.0,
        ci_low=float(low),
        ci_high=float(high),
        naive_ci_low=float(low),
        naive_ci_high=float(high),
        design_effect=1.0,
        effective_n=float(n),
        icc=0.0,
    )


def analyse_overall(
    df: pl.DataFrame, alpha: float
) -> tuple[list[ProportionEstimate], dict[str, float]]:
    """Estimate the headline proportions and the true-vs-control contrast.

    Args:
        df: Normalized polled-edges frame carrying the outcome column.
        alpha: Two-sided significance level.

    Returns:
        Tuple of (estimates, contrast statistics).
    """
    true_df = df.filter(pl.col("is_true_edge"))
    false_df = df.filter(~pl.col("is_true_edge"))

    true_supported = survey_estimates(true_df, alpha)[0]
    n_false = false_df.height
    false_unsupported = simple_estimate(
        "No literature support, negative controls",
        n_false - int(false_df[_OUTCOME].sum()),
        n_false,
        alpha,
    )

    p_false = int(false_df[_OUTCOME].sum()) / n_false if n_false else 0.0
    difference = true_supported.proportion - p_false
    contrast = {
        "true_supported": true_supported.proportion,
        "false_supported": p_false,
        "difference": difference,
        "effective_n_true": true_supported.effective_n,
        "effective_n_false": float(n_false),
        "power": float(
            power_proportions_2indep(
                diff=difference,
                prop2=p_false,
                nobs1=true_supported.effective_n,
                ratio=n_false / true_supported.effective_n,
                alpha=alpha,
                return_results=False,
            )
        ),
    }
    return [true_supported, false_unsupported], contrast


def build_precision_curve(
    proportion: float,
    deff: float,
    achieved_n: int,
    cost_per_edge: float,
    alpha: float,
) -> list[dict[str, float]]:
    """Tabulate achievable precision and cost across candidate sample sizes.

    Args:
        proportion: Proportion to evaluate precision around.
        deff: Design effect to apply.
        achieved_n: The sample size actually used.
        cost_per_edge: USD per evaluated edge.
        alpha: Two-sided significance level.

    Returns:
        One record per candidate sample size.
    """
    candidates = [int(achieved_n * m) for m in (0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0)]
    candidates.append(GRAPH_TOTAL_EDGES)

    curve: list[dict[str, float]] = []
    for n in candidates:
        effective_n = n / deff
        low, high = proportion_confint(
            round(proportion * effective_n),
            round(effective_n),
            alpha=alpha,
            method="wilson",
        )
        curve.append(
            {
                "n": float(n),
                "effective_n": effective_n,
                "half_width_pp": (high - low) / 2 * 100,
                "cost_usd": n * cost_per_edge,
                "is_achieved": float(n == achieved_n),
            }
        )
    return curve


def analyse_degree_coverage(
    centrality_path: Path, lower_percentile: float, upper_percentile: float
) -> list[CoverageRow]:
    """Quantify what the top/bottom degree-decile exclusion leaves in scope.

    Reports, per node type, the share of nodes retained and the share of degree
    mass retained. The two diverge sharply because degree is heavy-tailed:
    excluding the top decile removes a small number of nodes carrying most of
    the graph's edges.

    Args:
        centrality_path: CSV written by ``cli evals centrality``.
        lower_percentile: Lower cutoff (e.g. 10 excludes the bottom decile).
        upper_percentile: Upper cutoff (e.g. 90 excludes the top decile).

    Returns:
        One record per node type, plus an ``ALL`` total row.
    """
    if not centrality_path.exists():
        logger.warning(
            "Centrality file not found (%s); skipping coverage analysis.",
            centrality_path,
        )
        return []

    df = pl.read_csv(centrality_path, infer_schema_length=10000).with_columns(
        pl.col("centrality").cast(pl.Float64, strict=False)
    )

    rows: list[CoverageRow] = []
    total_mass = float(df["centrality"].sum())
    kept_mass = 0.0

    for label in sorted(df["label"].drop_nulls().unique().to_list()):
        group = df.filter(pl.col("label") == label)
        low = group["centrality"].quantile(lower_percentile / 100)
        high = group["centrality"].quantile(upper_percentile / 100)
        if low is None or high is None:
            continue
        kept = group.filter(
            (pl.col("centrality") > low) & (pl.col("centrality") < high)
        )
        mass = float(group["centrality"].sum())
        mass_kept = float(kept["centrality"].sum())
        kept_mass += mass_kept
        rows.append(
            CoverageRow(
                node_type=str(label),
                n_nodes=group.height,
                n_kept=kept.height,
                frac_nodes_kept=kept.height / group.height if group.height else 0.0,
                frac_degree_mass_kept=mass_kept / mass if mass else 0.0,
            )
        )

    rows.sort(key=lambda r: r.n_nodes, reverse=True)
    total_nodes = sum(r.n_nodes for r in rows)
    total_kept = sum(r.n_kept for r in rows)
    rows.append(
        CoverageRow(
            node_type="ALL",
            n_nodes=total_nodes,
            n_kept=total_kept,
            frac_nodes_kept=total_kept / total_nodes if total_nodes else 0.0,
            frac_degree_mass_kept=kept_mass / total_mass if total_mass else 0.0,
        )
    )
    return rows


def load_human_review(review_path: Path) -> pl.DataFrame:
    """Load the expert-review export, dropping unusable judgements.

    Rows the reviewer flagged as outside their domain of expertise
    (``abstained``) carry no judgement and are excluded so that they do not
    dilute the soundness denominators.

    Args:
        review_path: CSV exported from the blinded review form.

    Returns:
        DataFrame with ``soundness`` and ``is_true_edge`` coerced and
        abstentions removed.
    """
    df = pl.read_csv(review_path, infer_schema_length=10000).with_columns(
        pl.col("soundness").cast(pl.Int32, strict=False),
        pl.col("is_true_edge").cast(pl.Boolean, strict=False),
    )
    if "abstained" in df.columns:
        abstained = pl.col("abstained").cast(pl.Boolean, strict=False).fill_null(False)
        n_abstained = df.filter(abstained).height
        if n_abstained:
            logger.info(
                "Excluding %s abstained judgement(s) from the expert review.",
                n_abstained,
            )
        df = df.filter(~abstained)
    return df.filter(pl.col("soundness").is_not_null())


def analyse_human_review(
    review_path: Path | None, alpha: float
) -> list[ProportionEstimate]:
    """Precision of the blinded 100-edge expert review.

    Falls back to the counts reported in the reviewer response when no raw
    export is available. The expert sample is unclustered, so no design
    adjustment applies.

    Args:
        review_path: Optional CSV with ``soundness``, ``is_true_edge`` and
            ``rating_change`` columns.
        alpha: Two-sided significance level.

    Returns:
        One estimate per reported quantity.
    """
    if review_path is None or not review_path.exists():
        if review_path is not None:
            logger.warning(
                "Human-review file not found (%s); using reported summary counts.",
                review_path,
            )
        return [
            simple_estimate(label, k, n, alpha)
            for label, (k, n) in HUMAN_REVIEW_SUMMARY.items()
        ]

    df = load_human_review(review_path)
    sound = pl.col("soundness") >= SOUND_THRESHOLD
    true_edge = pl.col("is_true_edge").fill_null(False)

    estimates = [
        simple_estimate(
            "Reasoning sound (all edges)", df.filter(sound).height, df.height, alpha
        ),
        simple_estimate(
            "Reasoning sound (true edges)",
            df.filter(sound & true_edge).height,
            df.filter(true_edge).height,
            alpha,
        ),
        simple_estimate(
            "Reasoning sound (negative controls)",
            df.filter(sound & ~true_edge).height,
            df.filter(~true_edge).height,
            alpha,
        ),
    ]

    if "rating_change" in df.columns:
        estimates.extend(
            simple_estimate(
                label,
                df.filter(pl.col("rating_change") == key).height,
                df.height,
                alpha,
            )
            for label, key in [
                ("Reviewer would not change rating", "no_change"),
                ("Reviewer would increase rating", "increase"),
                ("Reviewer would decrease rating", "decrease"),
            ]
        )
    return estimates


def analyse_human_review_by_node_type(
    review_path: Path, alpha: float
) -> list[ProportionEstimate]:
    """Soundness of the agent's reasoning within each node type.

    Args:
        review_path: CSV exported from the blinded review form.
        alpha: Two-sided significance level.

    Returns:
        One estimate per node type, sorted by descending sample size.
    """
    df = load_human_review(review_path)
    if "seed_node_type" not in df.columns:
        return []

    sound = pl.col("soundness") >= SOUND_THRESHOLD
    estimates = [
        simple_estimate(
            str(node_type),
            df.filter((pl.col("seed_node_type") == node_type) & sound).height,
            df.filter(pl.col("seed_node_type") == node_type).height,
            alpha,
        )
        for node_type in df["seed_node_type"].drop_nulls().unique().to_list()
    ]
    return sorted(estimates, key=lambda e: e.n, reverse=True)


def analyse_rating_asymmetry(review_path: Path) -> dict[str, float]:
    """Test whether the agent is systematically conservative.

    Among edges where the expert recommended any change, an exact binomial
    (sign) test asks whether increases and decreases are equally likely. A
    significant excess of increases means the agent understates evidence more
    often than it overstates it, which makes the reported literature-support
    rate a conservative estimate rather than an inflated one.

    Args:
        review_path: CSV exported from the blinded review form.

    Returns:
        Counts, the proportion of changes that were increases, and the exact
        two-sided p-value.
    """
    df = load_human_review(review_path)
    if "rating_change" not in df.columns:
        return {}

    increases = df.filter(pl.col("rating_change") == "increase").height
    decreases = df.filter(pl.col("rating_change") == "decrease").height
    changed = increases + decreases
    if changed == 0:
        return {}

    return {
        "increases": float(increases),
        "decreases": float(decreases),
        "n_changed": float(changed),
        "proportion_increase": increases / changed,
        "p_value": float(stats.binomtest(increases, changed, 0.5).pvalue),
    }


def _log_estimates(heading: str, estimates: list[ProportionEstimate]) -> None:
    """Log a table of proportion estimates."""
    logger.info("")
    logger.info(_SEPARATOR)
    logger.info("  %s", heading)
    logger.info(_SEPARATOR)
    logger.info(
        "  %-38s %7s %8s %9s %7s  %s",
        "Estimate",
        "n",
        "n_eff",
        "estimate",
        "DEFF",
        "95% CI (design-adjusted)",
    )
    for est in estimates:
        flag = "" if est.is_usable else "  [descriptive only]"
        logger.info(
            "  %-38s %7s %8.0f %8.1f%% %7.2f  [%5.1f%%, %5.1f%%] ±%.1fpp%s",
            est.label[:38],
            f"{est.n:,}",
            est.effective_n,
            est.proportion * 100,
            est.design_effect,
            est.ci_low * 100,
            est.ci_high * 100,
            est.half_width_pp,
            flag,
        )


def _log_report(report: PowerReport) -> None:
    """Log the full report in human-readable form."""
    logger.info("")
    logger.info(_SEPARATOR)
    logger.info("  Power and precision analysis — %s", report.run_id)
    logger.info(_SEPARATOR)
    logger.info(
        "  Finite-population correction : %.5f  (sample vs %s graph edges)",
        report.finite_population_correction,
        f"{report.graph_total_edges:,}",
    )
    logger.info("  Interpretation               : the sampling fraction is immaterial;")
    logger.info(
        "                                 precision is set by absolute sample size."
    )

    _log_estimates("Headline estimates", report.overall)
    for est in report.overall:
        logger.info("")
        logger.info(
            "  %s: naive ±%.2fpp vs design-adjusted ±%.2fpp (ICC=%.3f, DEFF=%.2f)",
            est.label,
            est.naive_half_width_pp,
            est.half_width_pp,
            est.icc,
            est.design_effect,
        )

    contrast = report.contrast
    logger.info("")
    logger.info(_SEPARATOR)
    logger.info("  True edges vs negative controls")
    logger.info(_SEPARATOR)
    logger.info(
        "  %.1f%% vs %.1f%% supported — difference %.1fpp",
        contrast["true_supported"] * 100,
        contrast["false_supported"] * 100,
        contrast["difference"] * 100,
    )
    logger.info(
        "  Design-adjusted power to detect this difference: %.4f", contrast["power"]
    )

    _log_estimates("Literature support by seed node type", report.by_node_type)
    _log_estimates("Literature support by relation type", report.by_relation_type)
    _log_estimates("Expert human review", report.human_review)
    if report.human_review_by_node_type:
        _log_estimates(
            "Expert human review by node type", report.human_review_by_node_type
        )

    if report.rating_asymmetry:
        asym = report.rating_asymmetry
        logger.info("")
        logger.info(_SEPARATOR)
        logger.info("  Is the agent systematically conservative?")
        logger.info(_SEPARATOR)
        logger.info(
            "  Of %d edges where the expert recommended a change, %d were increases"
            " and %d decreases",
            int(asym["n_changed"]),
            int(asym["increases"]),
            int(asym["decreases"]),
        )
        logger.info(
            "  Exact binomial (sign) test vs 50/50: p = %.3g  (%.1f%% increases)",
            asym["p_value"],
            asym["proportion_increase"] * 100,
        )

    logger.info("")
    logger.info(_SEPARATOR)
    logger.info("  Precision vs sample size and cost")
    logger.info(_SEPARATOR)
    logger.info(
        "  %12s %12s %12s %16s", "edges", "effective n", "half-width", "cost (USD)"
    )
    for point in report.precision_curve:
        marker = "  <- achieved" if point["is_achieved"] else ""
        logger.info(
            "  %12s %12.0f %11.2fpp %16s%s",
            f"{int(point['n']):,}",
            point["effective_n"],
            point["half_width_pp"],
            f"${point['cost_usd']:,.0f}",
            marker,
        )

    if report.coverage:
        logger.info("")
        logger.info(_SEPARATOR)
        logger.info("  Sampling-frame coverage after degree-decile exclusion")
        logger.info(_SEPARATOR)
        logger.info(
            "  %-22s %10s %10s %12s %14s",
            "node type",
            "nodes",
            "eligible",
            "% nodes",
            "% degree mass",
        )
        for row in report.coverage:
            logger.info(
                "  %-22s %10s %10s %11.1f%% %13.1f%%",
                NODE_TYPE_LABELS.get(row.node_type, row.node_type),
                f"{row.n_nodes:,}",
                f"{row.n_kept:,}",
                row.frac_nodes_kept * 100,
                row.frac_degree_mass_kept * 100,
            )
    logger.info(_SEPARATOR)


def _style_axis(
    ax: plt.Axes, ink: str, muted: str, grid_axis: Literal["both", "x", "y"]
) -> None:
    """Apply the shared light/dark axis styling to a panel."""
    ax.tick_params(axis="both", colors=ink, labelsize=9)
    for side in ("left", "bottom"):
        ax.spines[side].set_edgecolor(ink)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis=grid_axis, alpha=0.2, color=muted)


def _plot_report(report: PowerReport, out_dir: Path, run_id: str) -> None:
    """Render the precision curve and per-node-type forest plot."""
    accent, warn = "#516FD9", "#DA3546"

    for theme_name, theme in THEMES.items():
        ink, muted = theme["ink"], theme["muted"]
        fig, axes = plt.subplots(1, 2, figsize=(12, 5), layout="tight")
        fig.patch.set_alpha(0)

        ax = axes[0]
        ax.patch.set_alpha(0)
        curve = [p for p in report.precision_curve if p["n"] <= GRAPH_TOTAL_EDGES]
        ax.plot(
            [p["n"] for p in curve],
            [p["half_width_pp"] for p in curve],
            color=accent,
            linewidth=2,
            zorder=2,
        )

        achieved = next((p for p in report.precision_curve if p["is_achieved"]), None)
        if achieved is not None:
            ax.scatter(
                [achieved["n"]], [achieved["half_width_pp"]], s=70, color=warn, zorder=3
            )
            ax.annotate(
                f"achieved\nn={int(achieved['n']):,}\n±{achieved['half_width_pp']:.1f}pp\n"
                f"≈${achieved['cost_usd']:,.0f}",
                xy=(achieved["n"], achieved["half_width_pp"]),
                xytext=(12, 18),
                textcoords="offset points",
                fontsize=8.5,
                color=ink,
            )

        ax.set_xscale("log")
        ax.set_xlabel("Edges evaluated", fontsize=10, color=ink)
        ax.set_ylabel("95% CI half-width (percentage points)", fontsize=10, color=ink)
        ax.set_title(
            "Precision scales with sample size, not sampling fraction",
            fontsize=11,
            color=ink,
            pad=10,
        )
        _style_axis(ax, ink, muted, grid_axis="y")

        ax = axes[1]
        ax.patch.set_alpha(0)
        estimates = list(reversed(report.by_node_type))
        for y, est in enumerate(estimates):
            color = accent if est.is_usable else muted
            ax.plot(
                [est.ci_low * 100, est.ci_high * 100],
                [y, y],
                color=color,
                linewidth=2,
                solid_capstyle="round",
            )
            ax.scatter([est.proportion * 100], [y], s=36, color=color, zorder=3)

        ax.axvline(
            report.overall[0].proportion * 100,
            color=warn,
            linestyle="--",
            linewidth=1,
            alpha=0.8,
        )
        ax.set_yticks(range(len(estimates)))
        ax.set_yticklabels(
            [
                f"{NODE_TYPE_LABELS.get(e.label, e.label)}  (n={e.n:,})"
                for e in estimates
            ],
            fontsize=9,
            color=ink,
        )
        ax.set_xlabel("Edges with literature support (%)", fontsize=10, color=ink)
        ax.set_title(
            "Design-adjusted 95% intervals by node type", fontsize=11, color=ink, pad=10
        )
        _style_axis(ax, ink, muted, grid_axis="x")

        out_path = out_dir / f"{run_id}_power_{theme_name}.svg"
        plt.savefig(out_path, transparent=True, bbox_inches="tight")
        if theme_name == "light":
            plt.savefig(
                out_dir / f"{run_id}_power.pdf", transparent=True, bbox_inches="tight"
            )
        plt.close(fig)
        logger.info("Saved %s", out_path)


def run(  # noqa: PLR0913
    input_path: Path,
    out_dir: Path | None = None,
    centrality_path: Path | None = None,
    human_review_path: Path | None = None,
    cost_per_edge: float = DEFAULT_COST_PER_EDGE_USD,
    alpha: float = DEFAULT_ALPHA,
    lower_percentile: float = 10.0,
    upper_percentile: float = 90.0,
) -> PowerReport:
    """Run the power and precision analysis for a PaperQA3 validation run.

    Args:
        input_path: Polled-edges CSV from ``cli evals paperqa --action poll``.
        out_dir: Where to write the JSON report and figures. Defaults to the
            directory containing ``input_path``.
        centrality_path: Centrality CSV used for the coverage analysis.
            Defaults to ``degree_undirected.csv`` beside the input.
        human_review_path: Optional raw expert-review export. Falls back to the
            counts reported in the reviewer response.
        cost_per_edge: USD per evaluated edge, for the cost-precision tradeoff.
        alpha: Two-sided significance level.
        lower_percentile: Lower degree cutoff applied during sampling.
        upper_percentile: Upper degree cutoff applied during sampling.

    Returns:
        The populated :class:`PowerReport`.
    """
    out_dir = out_dir or input_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = input_path.stem

    df = normalize_relation_types(load_polled_edges(input_path)).with_columns(
        (pl.col("rating") > RATING_NO_EVIDENCE).cast(pl.Int64).alias(_OUTCOME)
    )
    if df.is_empty():
        raise ValueError("No rows with a valid rating; nothing to analyse.")

    overall, contrast = analyse_overall(df, alpha)
    true_supported = overall[0]
    true_df = df.filter(pl.col("is_true_edge"))

    if centrality_path is None:
        centrality_path = input_path.parent / "degree_undirected.csv"
    has_review = human_review_path is not None and human_review_path.exists()

    report = PowerReport(
        run_id=run_id,
        alpha=alpha,
        cost_per_edge_usd=cost_per_edge,
        finite_population_correction=finite_population_correction(
            true_supported.n, GRAPH_TOTAL_EDGES
        ),
        graph_total_edges=GRAPH_TOTAL_EDGES,
        overall=overall,
        by_node_type=survey_estimates(true_df, alpha, by=_STRATUM),
        by_relation_type=(
            survey_estimates(true_df, alpha, by="relation_type")
            if "relation_type" in true_df.columns
            else []
        ),
        human_review=analyse_human_review(human_review_path, alpha),
        human_review_by_node_type=(
            analyse_human_review_by_node_type(human_review_path, alpha)
            if has_review
            else []
        ),
        rating_asymmetry=(
            analyse_rating_asymmetry(human_review_path) if has_review else {}
        ),
        contrast=contrast,
        precision_curve=build_precision_curve(
            true_supported.proportion,
            true_supported.design_effect,
            true_supported.n,
            cost_per_edge,
            alpha,
        ),
        coverage=analyse_degree_coverage(
            centrality_path, lower_percentile, upper_percentile
        ),
    )

    _log_report(report)
    _plot_report(report, out_dir, run_id)

    json_path = out_dir / f"{run_id}_power.json"
    json_path.write_text(json.dumps(asdict(report), indent=2, default=str))
    logger.info("Saved report to %s", json_path)

    return report
