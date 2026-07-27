"""Evaluation dataset computation for knowledge graph analysis."""

import logging
from enum import StrEnum
from pathlib import Path

import typer

from . import (
    centrality,
    human_review,
    human_review_stats,
    paperqa,
    paperqa_figures,
    sample_edges,
)

logger = logging.getLogger("cli")

evals_app = typer.Typer(help="Generate evaluation datasets for KG analysis.")


@evals_app.command(name="centrality", help="Compute node centrality scores.")
def centrality_cmd(  # noqa: PLR0913
    nodes_path: Path = typer.Option(
        Path("data/gold/kg/parquet/nodes.parquet"),
        "--nodes",
        help="Path to nodes.parquet file.",
    ),
    edges_path: Path = typer.Option(
        Path("data/gold/kg/parquet/edges.parquet"),
        "--edges",
        help="Path to edges.parquet file.",
    ),
    out_dir: Path = typer.Option(
        Path("data/gold/evals"),
        "--out",
        help="Directory to write outputs.",
    ),
    top_n: int = typer.Option(
        10,
        "--top",
        "-n",
        help="Number of top nodes to display.",
    ),
    metrics: list[str] = typer.Option(
        ["pagerank"],
        "--metric",
        help="Centrality metric (repeatable): pagerank, degree, betweenness, closeness, eigenvector.",
    ),
    alpha: float = typer.Option(
        0.85,
        "--alpha",
        help="PageRank damping factor (only used when --metric=pagerank).",
    ),
    graph_modes: list[str] = typer.Option(
        ["undirected"],
        "--graph-mode",
        help="Edge directionality (repeatable): undirected, directed.",
    ),
):
    """Compute node centrality scores for every (metric, graph-mode) combination.

    The graph is loaded once per unique --graph-mode; node metadata is loaded
    once. Each combination produces its own CSV and PDF.

    Examples:

        # Run with defaults (pagerank, undirected)
        uv run cli evals centrality

        # All metrics on a directed graph
        uv run cli evals centrality --metric pagerank --metric degree --graph-mode directed

        # Two metrics x two modes = four output files
        uv run cli evals centrality --metric pagerank --metric degree \\
            --graph-mode undirected --graph-mode directed

        # Show top 20 nodes, custom output directory
        uv run cli evals centrality --top 20 --out data/gold/evals/v2
    """
    centrality.run(nodes_path, edges_path, out_dir, top_n, alpha, metrics, graph_modes)


@evals_app.command(
    name="sample-edges", help="Generate edge evaluation dataset for link prediction."
)
def sample_edges_cmd(  # noqa: PLR0913
    nodes_path: Path = typer.Option(
        Path("data/gold/kg/parquet/nodes.parquet"),
        "--nodes",
        help="Path to nodes.parquet file.",
    ),
    edges_path: Path = typer.Option(
        Path("data/gold/kg/parquet/edges.parquet"),
        "--edges",
        help="Path to edges.parquet file.",
    ),
    out_dir: Path = typer.Option(
        Path("data/gold/evals"),
        "--out",
        help="Directory to write outputs.",
    ),
    metric: str = typer.Option(
        "pagerank",
        "--metric",
        help=(
            "Node-selection strategy. Use a centrality metric name "
            "(pagerank, degree, betweenness, closeness, eigenvector) to read a "
            "pre-computed centrality CSV from --out (raises an error if the file "
            "does not exist — run `cli evals centrality` first). "
            "Use 'uniform' to sample nodes uniformly at random within each node type."
        ),
    ),
    centrality_upper: int = typer.Option(
        None,
        "--centrality-upper",
        help="Upper percentile cutoff (top X%%). Ignored when --metric uniform. Overrides config.",
    ),
    centrality_lower: int = typer.Option(
        None,
        "--centrality-lower",
        help="Lower percentile cutoff (top X%%). Ignored when --metric uniform. Overrides config.",
    ),
    nodes_per_type: int = typer.Option(
        None,
        "--nodes-per-type",
        help="Nodes to sample per node type. Overrides config.",
    ),
    true_neighbors: int = typer.Option(
        None,
        "--true-neighbors",
        help="Max true neighbors to sample per node. Overrides config.",
    ),
    false_neighbors: int = typer.Option(
        None,
        "--false-neighbors",
        help="False neighbors to sample per node. Overrides config.",
    ),
    seed: int = typer.Option(
        None,
        "--seed",
        help="Random seed for reproducibility. Overrides config.",
    ),
    config_path: Path = typer.Option(
        None,
        "--config",
        help="Path to config file. Defaults to conf/base/evals.yml.",
    ),
):
    """Generate edge evaluation dataset for link prediction models.

    Selects seed nodes using --metric, then generates true/false edge pairs.
    Parameters are loaded from conf/base/evals.yml and can be overridden via
    CLI options.

    Outputs:
    - <metric>_<graph_mode>_distribution_by_type.pdf: Centrality vs rank plots (skipped for uniform)
    - sampled_nodes_<metric>[_lower=X_upper=Y].csv: Sampled seed nodes
    - sampled_edges_<metric>_true=N_false=M.csv: True and false edge pairs with labels
    - summary_stats.json: Sampling statistics

    Examples:

        # Sample using pre-computed PageRank (must run `cli evals centrality` first)
        uv run cli evals sample-edges --metric pagerank

        # Sample uniformly (no pre-computed centrality needed)
        uv run cli evals sample-edges --metric uniform

        # Override percentile range (centrality metrics only)
        uv run cli evals sample-edges --metric pagerank --centrality-upper 10 --centrality-lower 25

        # Use custom config file
        uv run cli evals sample-edges --config conf/local/evals.yml

        # Override specific parameters
        uv run cli evals sample-edges --metric uniform --nodes-per-type 200 --seed 123
    """
    sample_edges.run(
        nodes_path=nodes_path,
        edges_path=edges_path,
        out_dir=out_dir,
        metric=metric,
        centrality_upper=centrality_upper,
        centrality_lower=centrality_lower,
        nodes_per_type=nodes_per_type,
        true_neighbors=true_neighbors,
        false_neighbors=false_neighbors,
        seed=seed,
        config_path=config_path,
    )


class ActionType(StrEnum):
    submit = "submit"
    poll = "poll"


@evals_app.command(
    name="paperqa",
    help="Evaluate sampled edges using PaperQA3 via the Edison client.",
)
def paperqa_cmd(  # noqa: PLR0913
    input_path: Path = typer.Option(
        Path("data/gold/evals/sampled_edges_degree_true=10_false=1.csv"),
        "--input",
        help="Path to input CSV (sampled_edges CSV for 'submit', or a submitted_edges CSV for 'poll').",
    ),
    out_dir: Path = typer.Option(
        Path("data/gold/evals"),
        "--out",
        help="Output directory for results.",
    ),
    action: ActionType = typer.Option(
        ActionType.submit,
        "--action",
        help="Phase to execute: 'submit' to queue tasks, 'poll' to retrieve results.",
    ),
    limit: int = typer.Option(
        None,
        "--limit",
        help="Limit the number of edges to evaluate (useful for pilots).",
    ),
    wandb_project: str = typer.Option(
        "optimuskg-paperqa",
        "--wandb-project",
        help="Weights & Biases project name to log outputs to.",
    ),
    api_min_interval_sec: float = typer.Option(
        4.0,
        "--api-min-interval",
        help="Minimum seconds between Edison API calls.",
    ),
    max_rate_limit_attempts: int = typer.Option(
        15,
        "--max-rate-limit-attempts",
        help="Max retry attempts with exponential backoff on 429/5xx responses.",
    ),
):
    """Run literature-based evaluation of sampled edges.

    Examples:

        # Submit jobs (input must be a sampled_edges file)
        uv run cli evals paperqa --action submit --input data/gold/evals/sampled_edges_degree_true=10_false=5.csv

        # Poll for results (input must be the submitted_edges file from the submit step)
        uv run cli evals paperqa --action poll --input data/gold/evals/20260328_163632_submitted_edges.csv

        # Pilot run with a small subset
        uv run cli evals paperqa --action submit --limit 10

        # Log results to W&B on poll
        uv run cli evals paperqa --action poll --input data/gold/evals/20260328_163632_submitted_edges.csv --wandb-project my-project
    """
    paperqa.run(
        input_path=input_path,
        out_dir=out_dir,
        action=action.value,
        limit=limit,
        wandb_project=wandb_project,
        api_min_interval_sec=api_min_interval_sec,
        max_rate_limit_attempts=max_rate_limit_attempts,
    )


@evals_app.command(
    name="paperqa-figures", help="Generate rating bar plots from PaperQA3 results."
)
def paperqa_figures_cmd(
    input_path: Path = typer.Option(
        ...,
        "--input",
        help="Path to the polled-edges CSV produced by `cli evals paperqa --action poll`.",
    ),
    out_dir: Path = typer.Option(
        None,
        "--out",
        help="Output directory for the PDF. Defaults to the same directory as --input.",
    ),
):
    """Generate stacked bar plots of PaperQA3 rating distributions.

    Produces a two-panel PDF (false edges | true edges) with ratings on the
    x-axis, count on the y-axis, and bars stacked by seed node type.
    The output file is named ``<run_id>_paperqa_barplot.pdf``.

    Examples:

        uv run cli evals paperqa-figures --input data/gold/evals/20260328_173610_polled_edges.csv

        uv run cli evals paperqa-figures --input data/gold/evals/20260328_173610_polled_edges.csv --out figures/
    """
    paperqa_figures.run(
        input_path=input_path,
        out_dir=out_dir,
    )


@evals_app.command(
    name="make-review",
    help="Sample edges and build a shareable HTML form for expert review.",
)
def make_review_cmd(
    input_path: Path = typer.Option(
        Path("data/gold/evals/20260328_195935_polled_edges.csv"),
        "--input",
        help="Path to the polled-edges CSV produced by `cli evals paperqa --action poll`.",
    ),
    out_dir: Path = typer.Option(
        Path("data/gold/evals"),
        "--out",
        help="Directory to write the HTML form and sample CSV.",
    ),
    n: int = typer.Option(
        100,
        "--n",
        help="Number of edges to sample for review.",
    ),
    seed: int = typer.Option(
        42,
        "--seed",
        help="Random seed for reproducible sampling.",
    ),
):
    """Sample edges and generate a self-contained HTML expert-review form.

    Draws a reproducible, stratified (by seed node type) and true/false-balanced
    sample of edges from the polled-edges CSV, then writes a single self-contained
    HTML file to share with reviewers. Reviewers rate, on a 5-point
    Likert scale, whether the agent's reasoning about each edge is sound, then
    click "Download responses" to export a JSON file to send back.

    Outputs:
    - human_review_seed=<seed>_n=<n>.html: The shareable reviewer form.
    - human_review_seed=<seed>_n=<n>_sample.csv: Sampled edges (with hidden
      ground-truth labels) for the figures step.

    Examples:

        # Sample 100 edges with the default seed
        uv run cli evals make-review

        # Sample 150 edges with a different seed and input file
        uv run cli evals make-review --n 150 --seed 7 \\
            --input data/gold/evals/20260328_195935_polled_edges.csv
    """
    human_review.run_make_review(
        input_path=input_path,
        out_dir=out_dir,
        n=n,
        seed=seed,
    )


@evals_app.command(
    name="review-figures",
    help="Aggregate returned reviewer responses and generate figures.",
)
def review_figures_cmd(
    review_dir: Path = typer.Option(
        ...,
        "--review-dir",
        help="Run folder created by `make-review` (contains the *_sample.csv and responses/).",
    ),
    out_dir: Path = typer.Option(
        None,
        "--out",
        help="Output directory for figures and aggregated CSV. Defaults to --review-dir.",
    ),
):
    """Aggregate expert-review responses and generate summary figures.

    Reads the reviewer JSON files from ``<review-dir>/responses`` and the sample
    CSV from ``<review-dir>``, joins them, and produces a four-panel figure
    (soundness distribution by reviewer, human soundness vs. agent rating,
    soundness by ground-truth validity, inter-reviewer agreement) plus a
    long-format CSV.

    Outputs (written into --review-dir by default):
    - human_review_responses_long.csv: One row per (reviewer, edge) judgement.
    - human_review_figures.pdf / .svg: Four-panel summary figure.

    Examples:

        uv run cli evals review-figures \\
            --review-dir "data/gold/evals/human_review_seed=42_n=100"

        uv run cli evals review-figures \\
            --review-dir "data/gold/evals/human_review_seed=42_n=100" \\
            --out figures/
    """
    human_review.run_review_figures(
        review_dir=review_dir,
        out_dir=out_dir,
    )


@evals_app.command(
    name="review-stats",
    help="Compute reported statistics and publication figures from expert reviews.",
)
def review_stats_cmd(
    review_dir: Path = typer.Option(
        ...,
        "--review-dir",
        help="Run folder created by `make-review` (contains the *_sample.csv and responses/).",
    ),
    out_dir: Path = typer.Option(
        None,
        "--out",
        help="Output directory for statistics and figures. Defaults to --review-dir.",
    ),
):
    """Compute the statistics and publication figures quoted in a write-up.

    Reads the reviewer JSON files from ``<review-dir>/responses`` and the sample
    CSV from ``<review-dir>``, then reports the headline numbers (soundness,
    agent-rating adjustments, sensitivity vs. specificity, and reviewer--agent
    disagreement patterns) and renders clean, house-style figures. Unlike
    ``review-figures`` (exploratory multi-reviewer diagnostics), this command
    produces the compact, quotable outputs for a manuscript or reviewer response.

    Outputs (written into --review-dir by default):
    - human_review_evaluation.csv: Raw per-edge human evaluation (reviewer x edge).
    - rebuttal_stats.json: Every computed statistic.
    - rebuttal_stats.tex: \\newcommand macros to \\input into the response document.
    - figures/human_review_soundness.{png,pdf}: Soundness distribution.
    - figures/human_review_rating_change.{png,pdf}: Recommended rating adjustments.
    - figures/human_review_by_polarity.{png,pdf}: Sound rate, positive vs negative edges.
    - figures/human_review_main.{png,pdf}: Combined two-panel headline figure.

    Examples:

        uv run cli evals review-stats \\
            --review-dir "data/gold/evals/human_review_seed=42_n=100"

        uv run cli evals review-stats \\
            --review-dir "data/gold/evals/human_review_seed=42_n=100" \\
            --out figures/
    """
    human_review_stats.run_review_stats(
        review_dir=review_dir,
        out_dir=out_dir,
    )
