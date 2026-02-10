"""Knowledge graph metrics computation and export."""

import logging
from pathlib import Path

from .edges import compute_edge_metrics
from .nodes import compute_node_metrics
from .utils import load_parquet_dir

logger = logging.getLogger("cli")


def metrics_command(nodes_dir: Path, edges_dir: Path, out_dir: Path) -> None:
    """Load gold parquet files, compute metrics, and write parquet output.

    Reads individual node/edge parquet files from *nodes_dir* and *edges_dir*,
    computes per-label summary metrics, and writes:

    - ``out_dir/node_metrics.parquet``
    - ``out_dir/edge_metrics.parquet``
    """
    logger.info("Loading parquet files from %s and %s ...", nodes_dir, edges_dir)
    nodes = load_parquet_dir(nodes_dir)
    edges = load_parquet_dir(edges_dir)

    logger.info("Computing node metrics ...")
    node_metrics = compute_node_metrics(nodes, edges)

    logger.info("Computing edge metrics ...")
    edge_metrics = compute_edge_metrics(edges)

    out_dir.mkdir(parents=True, exist_ok=True)
    node_metrics.write_parquet(out_dir / "node_metrics.parquet")
    edge_metrics.write_parquet(out_dir / "edge_metrics.parquet")
    logger.info(
        "Metrics written to %s/node_metrics.parquet and %s/edge_metrics.parquet",
        out_dir,
        out_dir,
    )
