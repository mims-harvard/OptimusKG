import logging
from pathlib import Path

import typer

from cli.commands import (
    metrics_command,
    plot_benchmark_command,
    plot_normalized_time,
    sync_catalog_command,
    unify_benchmark_files_command,
)
from cli.commands.figures import figure_app
from evals.edge_eval import run as edge_eval_run
from evals.pagerank import run as pagerank_run
from optimuskg.utils import calculate_checksum

app = typer.Typer(help="Main entry point for the CLI.")
app.add_typer(figure_app, name="figure")

logger = logging.getLogger("cli")


@app.command(help="Log the checksum of a file or directory.")
def checksum(  # noqa: PLR0913
    path: Path,
    checksum: str = typer.Option(
        None, "--checksum", help="The checksum to compare the file to."
    ),
    chunk_size: int = typer.Option(
        8192, "--chunk-size", help="The size of the chunks to read from the file."
    ),
    digest_size: int = typer.Option(
        16, "--digest-size", help="The size of the digest to use for the checksum."
    ),
):
    try:
        actual_checksum = calculate_checksum(
            path=path,
            chunk_size=chunk_size,
            digest_size=digest_size,
        )

        display_path = f"directory '{path}'" if path.is_dir() else f"'{path}'"

        if not checksum:
            logger.info(f"The checksum of {display_path} is: {actual_checksum}")
        elif checksum == actual_checksum:
            logger.info(f"The checksum of {display_path} is correct")
        else:
            logger.error(
                f"The checksums do not match for {display_path}: {checksum} != {actual_checksum}"
            )
    except FileNotFoundError as e:
        logger.error(e)
    except IsADirectoryError as e:
        logger.error(e)
    except NotADirectoryError as e:
        logger.error(e)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


@app.command(help="Generate metrics parquet files from gold KG data.")
def metrics(
    nodes_dir: Path = typer.Option(
        "data/gold/formats/parquet/nodes",
        "--nodes",
        help="Directory containing gold node parquet files.",
    ),
    edges_dir: Path = typer.Option(
        "data/gold/formats/parquet/edges",
        "--edges",
        help="Directory containing gold edge parquet files.",
    ),
    out_dir: Path = typer.Option(
        "data/gold/metrics",
        "--out",
        help="Directory to write metrics parquet files to.",
    ),
):
    metrics_command(nodes_dir, edges_dir, out_dir)


@app.command(help="Plot benchmark results.")
def plot_benchmark(
    results_path: Path = typer.Option(
        "data/benchmarks/results.json",
        "--results",
        help="The path to read the results from.",
    ),
    out_dir: Path = typer.Option(
        "data/benchmarks/plots",
        "--out",
        help="The path to write the output file to.",
    ),
):
    plot_benchmark_command(results_path, out_dir)
    plot_normalized_time(
        "data/benchmarks/normalized_time/unified_benchmarks.json", out_dir
    )


@app.command(help="Unify benchmark files.")
def unify_benchmark_files(
    benchmarks_dir: Path = typer.Option(
        "data/benchmarks/normalized_time",
        "--benchmarks",
        help="The path to read the benchmarks from.",
    ),
):
    unify_benchmark_files_command(benchmarks_dir)


@app.command(help="Synchronize or validate catalog schemas and checksums.")
def sync_catalog(  # noqa: PLR0913
    layer: str = typer.Option(
        "all",
        "--layer",
        "-l",
        help="Target layer: landing, bronze, silver, or all.",
    ),
    dataset: str = typer.Option(
        None,
        "--dataset",
        "-d",
        help="Specific dataset name (e.g., bronze.opentargets.disease).",
    ),
    validate: bool = typer.Option(
        False,
        "--validate",
        "-v",
        help="Validate schemas and checksums without updating files.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        "-n",
        help="Preview changes without writing files.",
    ),
    catalog_dir: Path = typer.Option(
        Path("conf/base/catalog"),
        "--catalog-dir",
        help="Path to the catalog directory.",
    ),
    data_dir: Path = typer.Option(
        Path("data"),
        "--data-dir",
        help="Path to the data directory.",
    ),
):
    """Synchronize or validate catalog schemas and checksums.

    For ParquetDataset entries, reads the parquet file and updates the YAML
    schema specification.  For any dataset with a ``metadata.checksum``
    field, recomputes the checksum from the data file on disk and updates
    the catalog YAML (using regex replacement to preserve formatting).

    Examples:

        # Sync all schemas and checksums

        python -m cli sync-catalog

        # Validate without updating

        python -m cli sync-catalog --validate

        # Sync bronze layer only, dry-run

        python -m cli sync-catalog --layer bronze --dry-run

        # Sync a specific dataset

        python -m cli sync-catalog --dataset bronze.opentargets.disease
    """
    sync_catalog_command(
        layer=layer,
        dataset=dataset,
        validate=validate,
        dry_run=dry_run,
        catalog_dir=catalog_dir,
        data_dir=data_dir,
    )


@app.command(help="Compute PageRank importance scores for the knowledge graph.")
def pagerank(
    nodes_dir: Path = typer.Option(
        Path("data/silver/nodes"),
        "--nodes",
        help="Directory containing node parquet files.",
    ),
    edges_dir: Path = typer.Option(
        Path("data/silver/edges"),
        "--edges",
        help="Directory containing edge parquet files.",
    ),
    out_dir: Path = typer.Option(
        Path("evals/outputs"),
        "--out",
        help="Directory to write outputs (CSV, figures).",
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

    Builds an undirected graph from the silver layer edges, computes
    PageRank centrality for all nodes, and outputs:

    - Console table of top N nodes with names
    - CSV file with full rankings
    - PDF bar chart of mean PageRank by node type

    Examples:

        # Run with defaults
        uv run cli pagerank

        # Show top 20 nodes
        uv run cli pagerank --top 20

        # Custom output directory
        uv run cli pagerank --out evals/outputs/v2
    """
    pagerank_run(nodes_dir, edges_dir, out_dir, top_n, alpha)


@app.command(
    name="edge-eval", help="Generate edge evaluation dataset for link prediction."
)
def edge_eval(  # noqa: PLR0913
    nodes_dir: Path = typer.Option(
        Path("data/silver/nodes"),
        "--nodes",
        help="Directory containing node parquet files.",
    ),
    edges_dir: Path = typer.Option(
        Path("data/silver/edges"),
        "--edges",
        help="Directory containing edge parquet files.",
    ),
    out_dir: Path = typer.Option(
        Path("evals/outputs"),
        "--out",
        help="Directory to write outputs.",
    ),
    pagerank_upper: int = typer.Option(
        5,
        "--pagerank-upper",
        help="Upper percentile cutoff (top X%%).",
    ),
    pagerank_lower: int = typer.Option(
        15,
        "--pagerank-lower",
        help="Lower percentile cutoff (top X%%).",
    ),
    nodes_per_type: int = typer.Option(
        100,
        "--nodes-per-type",
        help="Nodes to sample per node type.",
    ),
    true_neighbors: int = typer.Option(
        10,
        "--true-neighbors",
        help="Max true neighbors to sample per node.",
    ),
    false_neighbors: int = typer.Option(
        5,
        "--false-neighbors",
        help="False neighbors to sample per node.",
    ),
    seed: int = typer.Option(
        42,
        "--seed",
        help="Random seed for reproducibility.",
    ),
):
    """Generate edge evaluation dataset for link prediction models.

    Samples nodes from the knowledge graph based on PageRank centrality
    (within a specified percentile range), then generates true/false edge
    pairs for evaluation.

    Outputs:
    - pagerank_distribution_by_type.pdf: PageRank vs rank plots per node type
    - sampled_nodes.csv: Sampled seed nodes with metadata
    - sampled_edges.csv: True and false edge pairs with labels
    - summary_stats.json: Sampling statistics

    Examples:

        # Run with defaults (top 5-15%, 100 nodes/type)
        uv run cli edge-eval

        # Custom percentile range
        uv run cli edge-eval --pagerank-upper 10 --pagerank-lower 25

        # More nodes per type
        uv run cli edge-eval --nodes-per-type 200

        # Custom output directory
        uv run cli edge-eval --out evals/outputs/experiment_v2
    """
    edge_eval_run(
        nodes_dir=nodes_dir,
        edges_dir=edges_dir,
        out_dir=out_dir,
        pagerank_upper=pagerank_upper,
        pagerank_lower=pagerank_lower,
        nodes_per_type=nodes_per_type,
        true_neighbors=true_neighbors,
        false_neighbors=false_neighbors,
        seed=seed,
    )


if __name__ == "__main__":
    app()
