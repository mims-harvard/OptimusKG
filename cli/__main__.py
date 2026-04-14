import logging
import warnings
from pathlib import Path

import typer

from optimuskg.utils import calculate_checksum

from .commands import (
    metrics_command,
    sync_catalog_command,
)
from .commands.evals import evals_app
from .commands.figures import figure_app

warnings.filterwarnings(
    "ignore",
    message="Dataset name '.*' contains '.' characters.*",
    category=UserWarning,
)

app = typer.Typer(help="Main entry point for the CLI.")
app.add_typer(evals_app, name="evals")
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
        "data/gold/kg/parquet/nodes",
        "--nodes",
        help="Directory containing gold node parquet files.",
    ),
    edges_dir: Path = typer.Option(
        "data/gold/kg/parquet/edges",
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


if __name__ == "__main__":
    app()
