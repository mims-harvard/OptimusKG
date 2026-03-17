"""Figure data computation and plot rendering."""

import logging
import warnings
from pathlib import Path

import polars as pl
import typer

# Suppress Kedro warning spam for dotted dataset names used by this project.
warnings.filterwarnings(
    "ignore",
    message=r"Dataset name '.*' contains '\.' characters.*",
    category=UserWarning,
)

from .registry import FIGURES

logger = logging.getLogger("cli")

figure_app = typer.Typer(help="Compute figure data and render plots.")


@figure_app.command(help="Compute figure-specific data and save as parquet.")
def data(
    figure: str = typer.Argument(help="Figure name (e.g. 'adjacency-heatmap')."),
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
        "data/gold/figures",
        "--out",
        help="Directory to write figure data parquet files to.",
    ),
):
    if figure not in FIGURES:
        available = ", ".join(sorted(FIGURES))
        logger.error("Unknown figure '%s'. Available: %s", figure, available)
        raise typer.Exit(code=1)

    module = FIGURES[figure]
    result = module.compute_data(nodes_dir, edges_dir)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{figure.replace('-', '_')}.parquet"
    result.write_parquet(out_path)
    logger.info("Figure data written to %s", out_path)


@figure_app.command(help="Render a figure from precomputed data and save as PDF.")
def plot(
    figure: str = typer.Argument(help="Figure name (e.g. 'adjacency-heatmap')."),
    data_dir: Path = typer.Option(
        "data/gold/figures",
        "--data",
        help="Directory containing figure data parquet files.",
    ),
    out_dir: Path = typer.Option(
        "data/gold/figures",
        "--out",
        help="Directory to write PDF plots to.",
    ),
):
    if figure not in FIGURES:
        available = ", ".join(sorted(FIGURES))
        logger.error("Unknown figure '%s'. Available: %s", figure, available)
        raise typer.Exit(code=1)

    module = FIGURES[figure]
    data_path = data_dir / f"{figure.replace('-', '_')}.parquet"
    df = pl.read_parquet(data_path)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{figure.replace('-', '_')}.pdf"
    module.render_plot(df, out_path)
    logger.info("Plot saved to %s", out_path)
