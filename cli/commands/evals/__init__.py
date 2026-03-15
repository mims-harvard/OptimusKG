"""Evaluation dataset computation for knowledge graph analysis."""

import logging
from pathlib import Path

import typer

from . import pagerank, sample_edges

logger = logging.getLogger("cli")

evals_app = typer.Typer(help="Generate evaluation datasets for KG analysis.")


@evals_app.command(name="pagerank", help="Compute PageRank importance scores.")
def pagerank_cmd(
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
    alpha: float = typer.Option(
        0.85,
        "--alpha",
        help="PageRank damping factor.",
    ),
):
    """Compute PageRank importance scores for the knowledge graph.

    Builds an undirected graph from the gold KG exports, computes
    PageRank centrality for all nodes, and outputs:

    - Console table of top N nodes with names
    - CSV file with full rankings
    - PDF bar chart of mean PageRank by node type

    Examples:

        # Run with defaults
        uv run cli evals pagerank

        # Show top 20 nodes
        uv run cli evals pagerank --top 20

        # Custom output directory
        uv run cli evals pagerank --out data/gold/evals/v2
    """
    pagerank.run(nodes_path, edges_path, out_dir, top_n, alpha)


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
    pagerank_upper: int = typer.Option(
        None,
        "--pagerank-upper",
        help="Upper percentile cutoff (top X%%). Overrides config.",
    ),
    pagerank_lower: int = typer.Option(
        None,
        "--pagerank-lower",
        help="Lower percentile cutoff (top X%%). Overrides config.",
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

    Samples nodes from the knowledge graph based on PageRank centrality
    (within a specified percentile range), then generates true/false edge
    pairs for evaluation. Parameters are loaded from conf/base/evals.yml
    and can be overridden via CLI options.

    Outputs:
    - pagerank_distribution_by_type.pdf: PageRank vs rank plots per node type
    - sampled_nodes.csv: Sampled seed nodes with metadata
    - sampled_edges.csv: True and false edge pairs with labels
    - summary_stats.json: Sampling statistics

    Examples:

        # Run with config defaults
        uv run cli evals sample-edges

        # Override percentile range
        uv run cli evals sample-edges --pagerank-upper 10 --pagerank-lower 25

        # Use custom config file
        uv run cli evals sample-edges --config conf/local/evals.yml

        # Override specific parameters
        uv run cli evals sample-edges --nodes-per-type 200 --seed 123
    """
    sample_edges.run(
        nodes_path=nodes_path,
        edges_path=edges_path,
        out_dir=out_dir,
        pagerank_upper=pagerank_upper,
        pagerank_lower=pagerank_lower,
        nodes_per_type=nodes_per_type,
        true_neighbors=true_neighbors,
        false_neighbors=false_neighbors,
        seed=seed,
        config_path=config_path,
    )
