"""Knowledge-graph embeddings: TransE training, PCA/UMAP, cluster metrics.

Workflow (each step is a sub-command):

* ``download`` — fetch the gold graph from Dataverse via the optimuskg client.
* ``train``    — learn TransE entity + relation embeddings; write artifacts.
* ``analyze``  — project to 2D (PCA/UMAP), render scatters, and compute the
  cluster-coherence metrics that quantify whether same-type entities/relations
  co-locate in latent space.
* ``run``      — ``train`` followed by ``analyze`` in one go.

Heavy dependencies (torch, scikit-learn, umap-learn) live in the optional
``embeddings`` dependency group and are imported lazily (via :mod:`runner`)
inside each command, so importing this sub-app never forces them on the rest of
the CLI. Install them with ``uv sync --group embeddings``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer

logger = logging.getLogger("cli")

embeddings_app = typer.Typer(
    help="Train TransE embeddings and assess type clustering (PCA/UMAP + metrics)."
)

# Canonical gold-graph locations (shared with `cli evals`).
_NODES = Path("data/gold/kg/parquet/nodes.parquet")
_EDGES = Path("data/gold/kg/parquet/edges.parquet")
_OUT = Path("data/gold/embeddings")


@embeddings_app.command(help="Download the gold graph via the optimuskg client.")
def download(
    nodes_path: Path = typer.Option(
        _NODES, "--nodes", help="Destination for the consolidated nodes.parquet."
    ),
    edges_path: Path = typer.Option(
        _EDGES, "--edges", help="Destination for the consolidated edges.parquet."
    ),
    lcc: bool = typer.Option(
        False, "--lcc/--full", help="Download only the largest connected component."
    ),
    cache_dir: Path = typer.Option(
        None,
        "--cache-dir",
        help="Client cache directory (default: <nodes dir>/.optimuskg_cache).",
    ),
    doi: str = typer.Option(
        None, "--doi", help="Dataverse DOI to target (default: published graph)."
    ),
):
    """Fetch ``nodes.parquet`` / ``edges.parquet`` from Harvard Dataverse.

    Runs the published ``optimuskg`` client in an isolated subprocess (its import
    name collides with this repo's package), caching under ``data/`` by default.
    """
    from . import runner  # noqa: PLC0415

    runner.download_gold(nodes_path, edges_path, lcc=lcc, cache_dir=cache_dir, doi=doi)


@embeddings_app.command(help="Train TransE entity and relation embeddings.")
def train(  # noqa: PLR0913
    nodes_path: Path = typer.Option(
        _NODES, "--nodes", help="Path to nodes.parquet (downloaded if missing)."
    ),
    edges_path: Path = typer.Option(
        _EDGES, "--edges", help="Path to edges.parquet (downloaded if missing)."
    ),
    out_dir: Path = typer.Option(
        _OUT, "--out", help="Directory to write embedding artifacts."
    ),
    relation_key: str = typer.Option(
        "relation",
        "--relation-key",
        help="Relation column: 'relation' (~40 semantic) or 'label' (27 metaedge).",
    ),
    lcc: bool = typer.Option(
        False, "--lcc/--full", help="Use only the largest connected component."
    ),
    download: bool = typer.Option(
        True, "--download/--no-download", help="Auto-download gold data if missing."
    ),
    max_edges: int = typer.Option(
        None, "--max-edges", help="Subsample to this many edges (quick runs)."
    ),
    dim: int = typer.Option(128, "--dim", help="Embedding dimensionality."),
    epochs: int = typer.Option(10, "--epochs", help="Training epochs."),
    batch_size: int = typer.Option(8192, "--batch-size", help="Mini-batch size."),
    lr: float = typer.Option(0.01, "--lr", help="Adam learning rate."),
    margin: float = typer.Option(
        1.0, "--margin", help="Margin gamma in the ranking loss."
    ),
    p_norm: int = typer.Option(1, "--p-norm", help="Dissimilarity norm (1=L1, 2=L2)."),
    negatives: int = typer.Option(
        1,
        "--negatives",
        help="Negative samples per positive (use >=16 for adversarial).",
    ),
    loss: str = typer.Option(
        "margin",
        "--loss",
        help="Loss: 'adversarial' (self-adversarial, best), 'max_margin', or 'margin'.",
    ),
    adv_temperature: float = typer.Option(
        1.0,
        "--adv-temperature",
        help="Self-adversarial softmax temperature (loss=adversarial).",
    ),
    device: str = typer.Option("auto", "--device", help="auto | cpu | cuda | mps."),
    seed: int = typer.Option(42, "--seed", help="Random seed."),
):
    """Train TransE and write embeddings + metadata to ``--out``.

    Examples:

        # Full graph, classic margin loss (lightweight baseline)
        uv run --group embeddings cli embeddings train

        # Higher quality: self-adversarial negatives (the recommended setup)
        uv run --group embeddings cli embeddings train --loss adversarial \\
            --negatives 32 --margin 12 --lr 0.005 --epochs 15

        # Fast smoke run on the LCC with a subsample
        uv run --group embeddings cli embeddings train --lcc --max-edges 500000 --epochs 5
    """
    from . import runner  # noqa: PLC0415
    from .transe import TransEConfig  # noqa: PLC0415

    config = TransEConfig(
        dim=dim,
        epochs=epochs,
        batch_size=batch_size,
        lr=lr,
        margin=margin,
        p_norm=p_norm,
        negatives=negatives,
        loss=loss,
        adv_temperature=adv_temperature,
        seed=seed,
        device=device,
    )
    runner.train_embeddings(
        nodes_path,
        edges_path,
        out_dir,
        relation_key=relation_key,
        lcc=lcc,
        download=download,
        max_edges=max_edges,
        config=config,
    )


@embeddings_app.command(
    help="Project embeddings (PCA/UMAP) and compute cluster metrics."
)
def analyze(  # noqa: PLR0913
    out_dir: Path = typer.Option(
        _OUT, "--out", help="Directory holding train artifacts; outputs written here."
    ),
    fmt: Annotated[
        str, typer.Option("--format", "-f", help="Plot format: pdf or svg.")
    ] = "pdf",
    umap: bool = typer.Option(
        True, "--umap/--no-umap", help="Also compute the (slower) UMAP projection."
    ),
    umap_sample: int = typer.Option(
        50000, "--umap-sample", help="Max entities fed to UMAP."
    ),
    plot_max_points: int = typer.Option(
        60000, "--plot-points", help="Max entity points drawn per scatter."
    ),
    metric_sample: int = typer.Option(
        10000, "--metric-sample", help="Subsample for O(n^2) metrics (silhouette)."
    ),
    knn_sample: int = typer.Option(
        20000, "--knn-sample", help="Subsample for the k-NN probe and k-means."
    ),
    knn_k: int = typer.Option(
        15, "--knn-k", help="Neighbours for the k-NN type probe."
    ),
    umap_neighbors: int = typer.Option(
        30,
        "--umap-neighbors",
        help="UMAP n_neighbors (larger = more global structure, closes gaps).",
    ),
    umap_min_dist: float = typer.Option(
        0.9,
        "--umap-min-dist",
        help="UMAP min_dist (larger = looser/rounder, less dense clusters).",
    ),
    umap_spread: float = typer.Option(
        1.5,
        "--umap-spread",
        help="UMAP spread / overall scale (raise for looser clusters).",
    ),
    umap_metric: str = typer.Option(
        "cosine", "--umap-metric", help="UMAP distance metric (e.g. cosine, euclidean)."
    ),
    seed: int = typer.Option(42, "--seed", help="Random seed."),
):
    """Render PCA/UMAP scatters and write ``cluster_metrics.json``.

    Metrics are computed on the full-dimensional embeddings; the 2D projections
    are for visualisation and are themselves scored with trustworthiness /
    continuity so the figure can be reported honestly. Re-run this command alone
    (no retraining) to tweak the UMAP projection.

    Examples:

        # Re-project the already-trained embeddings with different UMAP settings
        uv run --group embeddings cli embeddings analyze --umap-neighbors 50 --umap-min-dist 0.3
    """
    from . import runner  # noqa: PLC0415
    from .project import UmapParams  # noqa: PLC0415

    runner.analyze_embeddings(
        out_dir,
        fmt=fmt,
        do_umap=umap,
        umap_sample=umap_sample,
        umap_params=UmapParams(
            n_neighbors=umap_neighbors,
            min_dist=umap_min_dist,
            spread=umap_spread,
            metric=umap_metric,
        ),
        plot_max_points=plot_max_points,
        metric_sample=metric_sample,
        knn_sample=knn_sample,
        knn_k=knn_k,
        seed=seed,
    )


@embeddings_app.command(help="Train then analyze in one command.")
def run(  # noqa: PLR0913
    out_dir: Path = typer.Option(_OUT, "--out", help="Output directory."),
    relation_key: str = typer.Option(
        "relation", "--relation-key", help="'relation' or 'label'."
    ),
    lcc: bool = typer.Option(
        False, "--lcc/--full", help="Use only the largest connected component."
    ),
    max_edges: int = typer.Option(
        None, "--max-edges", help="Subsample edges (quick runs)."
    ),
    dim: int = typer.Option(128, "--dim", help="Embedding dimensionality."),
    epochs: int = typer.Option(10, "--epochs", help="Training epochs."),
    umap: bool = typer.Option(True, "--umap/--no-umap", help="Also compute UMAP."),
    device: str = typer.Option("auto", "--device", help="auto | cpu | cuda | mps."),
    seed: int = typer.Option(42, "--seed", help="Random seed."),
):
    """Convenience wrapper: ``train`` with defaults, then ``analyze``."""
    from . import runner  # noqa: PLC0415
    from .transe import TransEConfig  # noqa: PLC0415

    config = TransEConfig(dim=dim, epochs=epochs, seed=seed, device=device)
    runner.train_embeddings(
        _NODES,
        _EDGES,
        out_dir,
        relation_key=relation_key,
        lcc=lcc,
        download=True,
        max_edges=max_edges,
        config=config,
    )
    runner.analyze_embeddings(
        out_dir,
        fmt="pdf",
        do_umap=umap,
        umap_sample=50000,
        plot_max_points=60000,
        metric_sample=10000,
        knn_sample=20000,
        knn_k=15,
        seed=seed,
    )
