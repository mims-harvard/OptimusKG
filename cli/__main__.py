import logging
from pathlib import Path

import typer

from cli.commands import (
    get_primekg_metrics_command,
    metrics_command,
    plot_benchmark_command,
    plot_normalized_time,
    sync_catalog_schemas_command,
    unify_benchmark_files_command,
)
from optimuskg.utils import calculate_checksum

app = typer.Typer(help="Main entry point for the CLI.")

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


@app.command(help="Get statistics about a PrimeKG knowledge graph.")
def primekg_metrics(
    in_path: Path = typer.Option(
        "data/primekg/kg.csv",
        "--in",
        help="The path to read the input file from.",
    ),
    out_path: Path = typer.Option(
        "data/primekg/metrics.json",
        "--out",
        help="The path to write the output file to.",
    ),
):
    get_primekg_metrics_command(in_path, out_path)


@app.command(help="Generate metrics report from OptimusKG parquet files.")
def metrics(
    nodes_dir: Path = typer.Option(
        "data/silver/nodes",
        "--nodes",
        help="The path to read the nodes from.",
    ),
    edges_dir: Path = typer.Option(
        "data/silver/edges",
        "--edges",
        help="The path to read the edges from.",
    ),
    out_dir: Path = typer.Option(
        "data/gold/metrics",
        "--out",
        help="The path to write the output file to.",
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


@app.command(help="Synchronize or validate schema specifications for parquet datasets.")
def sync_schemas(
    layer: str = typer.Option(
        "all",
        "--layer",
        "-l",
        help="Target layer: bronze, silver, or all.",
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
        help="Validate schemas without updating files.",
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
    """Synchronize or validate schema specifications for parquet datasets.

    This command reads parquet files and generates idiomatic YAML schema
    specifications for the Kedro catalog files.

    Examples:

        # Sync all parquet schemas

        python -m cli sync-catalog-schemas

        # Validate without updating

        python -m cli sync-catalog-schemas --validate

        # Sync bronze layer only, dry-run

        python -m cli sync-catalog-schemas --layer bronze --dry-run

        # Sync a specific dataset

        python -m cli sync-catalog-schemas --dataset bronze.opentargets.disease
    """
    sync_catalog_schemas_command(
        layer=layer,
        dataset=dataset,
        validate=validate,
        dry_run=dry_run,
        catalog_dir=catalog_dir,
        data_dir=data_dir,
    )


if __name__ == "__main__":
    app()
