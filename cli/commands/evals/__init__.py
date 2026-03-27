"""Evaluation dataset computation for knowledge graph analysis."""

import logging
from pathlib import Path

import typer

from . import centrality, paperqa, sample_edges

logger = logging.getLogger("cli")

evals_app = typer.Typer(help="Generate evaluation datasets for KG analysis.")


@evals_app.command(name="centrality", help="Compute node centrality scores.")
def centrality_cmd(
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


@evals_app.command(
    name="paperqa",
    help="Evaluate sampled edges using PaperQA3 via the Edison client.",
)
def paperqa_cmd(
    sampled_edges_path: Path = typer.Option(
        Path("data/gold/evals/sampled_edges.csv"),
        "--edges",
        help="Path to sampled_edges.csv produced by `cli evals sample-edges`.",
    ),
    out_path: Path = typer.Option(
        Path("data/gold/evals/sampled_edges_paperqa.csv"),
        "--out",
        help="Output CSV path with PaperQA3 answers added.",
    ),
):
    """Run literature-based evaluation of sampled edges.

    This command expects the edge evaluation dataset produced by:

        uv run cli evals sample-edges

    It constructs prompts of the form:

        Is there any scientific or medical evidence to support an association
        between the <seed_type> <seed_name> and the <target_type> <target_name>?

    and submits them to the Edison Platform (PaperQA3 / JobNames.LITERATURE).
    """
    paperqa.run(
        sampled_edges_path=sampled_edges_path,
        out_path=out_path,
    )

