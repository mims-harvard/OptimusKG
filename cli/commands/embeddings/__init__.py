"""Knowledge-graph embeddings: TransE training, PCA/UMAP, cluster metrics.

Workflow (each step is a sub-command):

* ``download`` — fetch the gold graph from Dataverse via the optimuskg client.
* ``train``    — learn TransE entity + relation embeddings; write artifacts.
* ``analyze``  — project to 2D (PCA/UMAP), render scatters, and compute the
  cluster-coherence metrics that quantify whether same-type entities/relations
  co-locate in latent space.
* ``run``      — ``train`` followed by ``analyze`` in one go.

Heavy dependencies (torch, scikit-learn, umap-learn) live in the optional
``embeddings`` dependency group and are imported lazily inside each command, so
importing this sub-app never forces them on the rest of the CLI. Install them
with ``uv sync --group embeddings``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Annotated

import typer

logger = logging.getLogger("cli")

embeddings_app = typer.Typer(
    help="Train TransE embeddings and assess type clustering (PCA/UMAP + metrics)."
)

# Artifact filenames written by `train` and read by `analyze`.
ENTITY_EMB = "entity_emb.npy"
RELATION_EMB = "relation_emb.npy"
ENTITIES = "entities.parquet"
RELATIONS = "relations.parquet"
TRAIN_LOG = "train_log.json"
METRICS = "cluster_metrics.json"


# ---------------------------------------------------------------------------
# Sub-commands
# ---------------------------------------------------------------------------


@embeddings_app.command(help="Download the gold graph via the optimuskg client.")
def download(
    nodes_path: Path = typer.Option(
        Path("data/gold/kg/parquet/nodes.parquet"), "--nodes",
        help="Destination for the consolidated nodes.parquet.",
    ),
    edges_path: Path = typer.Option(
        Path("data/gold/kg/parquet/edges.parquet"), "--edges",
        help="Destination for the consolidated edges.parquet.",
    ),
    lcc: bool = typer.Option(
        False, "--lcc/--full",
        help="Download only the largest connected component.",
    ),
    cache_dir: Path = typer.Option(
        None, "--cache-dir",
        help="Client cache directory (default: <nodes dir>/.optimuskg_cache).",
    ),
    doi: str = typer.Option(
        None, "--doi", help="Dataverse DOI to target (default: published graph).",
    ),
):
    """Fetch ``nodes.parquet`` / ``edges.parquet`` from Harvard Dataverse.

    Runs the published ``optimuskg`` client in an isolated subprocess (its import
    name collides with this repo's package), caching under ``data/`` by default.
    """
    from .data import DEFAULT_DOI, ensure_gold_data

    ensure_gold_data(
        nodes_path, edges_path, lcc=lcc, download=True,
        cache_dir=cache_dir, doi=doi or DEFAULT_DOI,
    )


@embeddings_app.command(help="Train TransE entity and relation embeddings.")
def train(  # noqa: PLR0913
    nodes_path: Path = typer.Option(
        Path("data/gold/kg/parquet/nodes.parquet"), "--nodes",
        help="Path to nodes.parquet (downloaded if missing).",
    ),
    edges_path: Path = typer.Option(
        Path("data/gold/kg/parquet/edges.parquet"), "--edges",
        help="Path to edges.parquet (downloaded if missing).",
    ),
    out_dir: Path = typer.Option(
        Path("data/gold/embeddings"), "--out",
        help="Directory to write embedding artifacts.",
    ),
    relation_key: str = typer.Option(
        "relation", "--relation-key",
        help="Edge column used as the relation: 'relation' (~40 semantic) or 'label' (27 metaedge).",
    ),
    lcc: bool = typer.Option(False, "--lcc/--full", help="Use only the largest connected component."),
    download: bool = typer.Option(True, "--download/--no-download", help="Auto-download gold data if missing."),
    max_edges: int = typer.Option(None, "--max-edges", help="Subsample to this many edges (quick runs)."),
    dim: int = typer.Option(128, "--dim", help="Embedding dimensionality."),
    epochs: int = typer.Option(10, "--epochs", help="Training epochs."),
    batch_size: int = typer.Option(8192, "--batch-size", help="Mini-batch size."),
    lr: float = typer.Option(0.01, "--lr", help="Adam learning rate."),
    margin: float = typer.Option(1.0, "--margin", help="Margin gamma in the ranking loss."),
    p_norm: int = typer.Option(1, "--p-norm", help="Dissimilarity norm (1=L1, 2=L2)."),
    negatives: int = typer.Option(1, "--negatives", help="Negative samples per positive."),
    device: str = typer.Option("auto", "--device", help="auto | cpu | cuda | mps."),
    seed: int = typer.Option(42, "--seed", help="Random seed."),
):
    """Train TransE and write embeddings + metadata to ``--out``.

    Examples:

        # Full graph, defaults (auto-downloads the gold data on first run)
        uv run --group embeddings cli embeddings train

        # Fast smoke run on the LCC with a subsample
        uv run --group embeddings cli embeddings train --lcc --max-edges 500000 --epochs 5
    """
    from .data import ensure_gold_data, load_triples
    from .transe import TransEConfig, train_transe

    nodes_path, edges_path = ensure_gold_data(
        nodes_path, edges_path, lcc=lcc, download=download
    )
    triples = load_triples(
        nodes_path, edges_path, relation_key=relation_key, max_edges=max_edges, seed=seed
    )
    config = TransEConfig(
        dim=dim, epochs=epochs, batch_size=batch_size, lr=lr, margin=margin,
        p_norm=p_norm, negatives=negatives, seed=seed, device=device,
    )
    entity_emb, relation_emb, log = train_transe(
        triples.head, triples.relation, triples.tail,
        triples.n_entities, triples.n_relations, config,
    )
    log["relation_key"] = relation_key
    log["lcc"] = lcc
    _save_artifacts(out_dir, triples, entity_emb, relation_emb, log)
    logger.info("Wrote embedding artifacts to %s", out_dir)


@embeddings_app.command(help="Project embeddings (PCA/UMAP) and compute cluster metrics.")
def analyze(  # noqa: PLR0913
    out_dir: Path = typer.Option(
        Path("data/gold/embeddings"), "--out",
        help="Directory holding train artifacts; plots/metrics written here.",
    ),
    fmt: Annotated[str, typer.Option("--format", "-f", help="Plot format: pdf or svg.")] = "pdf",
    umap: bool = typer.Option(True, "--umap/--no-umap", help="Also compute the (slower) UMAP projection."),
    umap_sample: int = typer.Option(50000, "--umap-sample", help="Max entities fed to UMAP."),
    plot_max_points: int = typer.Option(60000, "--plot-points", help="Max entity points drawn per scatter."),
    metric_sample: int = typer.Option(10000, "--metric-sample", help="Subsample for O(n^2) metrics (silhouette)."),
    knn_sample: int = typer.Option(20000, "--knn-sample", help="Subsample for the k-NN probe and k-means."),
    knn_k: int = typer.Option(15, "--knn-k", help="Neighbours for the k-NN type probe."),
    seed: int = typer.Option(42, "--seed", help="Random seed."),
):
    """Render PCA/UMAP scatters and write ``cluster_metrics.json``.

    Metrics are computed on the full-dimensional embeddings; the 2D projections
    are for visualisation and are themselves scored with trustworthiness /
    continuity so the figure can be reported honestly.
    """
    _analyze(
        out_dir, fmt=fmt, do_umap=umap, umap_sample=umap_sample,
        plot_max_points=plot_max_points, metric_sample=metric_sample,
        knn_sample=knn_sample, knn_k=knn_k, seed=seed,
    )


@embeddings_app.command(help="Train then analyze in one command.")
def run(  # noqa: PLR0913
    out_dir: Path = typer.Option(Path("data/gold/embeddings"), "--out", help="Output directory."),
    relation_key: str = typer.Option("relation", "--relation-key", help="'relation' or 'label'."),
    lcc: bool = typer.Option(False, "--lcc/--full", help="Use only the largest connected component."),
    max_edges: int = typer.Option(None, "--max-edges", help="Subsample edges (quick runs)."),
    dim: int = typer.Option(128, "--dim", help="Embedding dimensionality."),
    epochs: int = typer.Option(10, "--epochs", help="Training epochs."),
    umap: bool = typer.Option(True, "--umap/--no-umap", help="Also compute UMAP."),
    device: str = typer.Option("auto", "--device", help="auto | cpu | cuda | mps."),
    seed: int = typer.Option(42, "--seed", help="Random seed."),
):
    """Convenience wrapper: ``train`` with defaults, then ``analyze``."""
    train(
        out_dir=out_dir, relation_key=relation_key, lcc=lcc, max_edges=max_edges,
        dim=dim, epochs=epochs, device=device, seed=seed,
    )
    _analyze(out_dir, fmt="pdf", do_umap=umap, umap_sample=50000,
             plot_max_points=60000, metric_sample=10000, knn_sample=20000,
             knn_k=15, seed=seed)


# ---------------------------------------------------------------------------
# Artifact IO + analysis orchestration
# ---------------------------------------------------------------------------


def _save_artifacts(out_dir: Path, triples, entity_emb, relation_emb, log: dict) -> None:
    """Persist embeddings, aligned metadata, and the training log."""
    import numpy as np
    import polars as pl

    from .data import relation_family

    out_dir.mkdir(parents=True, exist_ok=True)
    np.save(out_dir / ENTITY_EMB, entity_emb)
    np.save(out_dir / RELATION_EMB, relation_emb)
    pl.DataFrame({"id": triples.entity_ids, "type": triples.entity_types}).write_parquet(
        out_dir / ENTITIES
    )
    pl.DataFrame(
        {
            "relation": triples.relation_names,
            "family": [relation_family(r) for r in triples.relation_names],
        }
    ).write_parquet(out_dir / RELATIONS)
    (out_dir / TRAIN_LOG).write_text(json.dumps(log, indent=2))


def _analyze(  # noqa: PLR0913
    out_dir: Path, *, fmt: str, do_umap: bool, umap_sample: int,
    plot_max_points: int, metric_sample: int, knn_sample: int, knn_k: int, seed: int,
) -> None:
    """Load artifacts, project, plot, and compute the metric suite."""
    import numpy as np
    import polars as pl

    from . import metrics as M
    from . import project as P

    if fmt.lower() not in ("pdf", "svg"):
        raise typer.BadParameter("--format must be 'pdf' or 'svg'")
    ext = fmt.lower()

    entity_emb = np.load(out_dir / ENTITY_EMB)
    relation_emb = np.load(out_dir / RELATION_EMB)
    ent_meta = pl.read_parquet(out_dir / ENTITIES)
    rel_meta = pl.read_parquet(out_dir / RELATIONS)
    types = ent_meta["type"].to_list()
    relation_names = rel_meta["relation"].to_list()
    families = rel_meta["family"].to_list()

    report: dict = {}

    # --- Entities -----------------------------------------------------------
    logger.info("Analyzing %d entity embeddings...", entity_emb.shape[0])
    report["entities"] = M.cluster_report(
        entity_emb, types, sample_size=metric_sample,
        knn_sample_size=knn_sample, knn_k=knn_k, seed=seed,
    )

    pca_coords, ev = P.pca_project(entity_emb, seed=seed)
    P.plot_entity_scatter(
        pca_coords, types, out_dir / f"entities_pca.{ext}",
        method="PCA", explained=ev, max_points=plot_max_points, seed=seed,
    )
    _write_coords(out_dir / "entities_pca.parquet", ent_meta, pca_coords)
    report["projection_quality"] = {
        "pca": M.projection_quality(entity_emb, pca_coords, sample_size=metric_sample, seed=seed)
    }
    if "silhouette_per_type" in report["entities"]:
        P.plot_silhouette_bar(
            report["entities"]["silhouette_per_type"], out_dir / f"silhouette_by_type.{ext}"
        )

    if do_umap:
        sel = _subsample_idx(entity_emb.shape[0], umap_sample, seed)
        logger.info("Computing UMAP on %d entities...", sel.shape[0])
        umap_coords = P.umap_project(entity_emb[sel], seed=seed)
        P.plot_entity_scatter(
            umap_coords, [types[i] for i in sel], out_dir / f"entities_umap.{ext}",
            method="UMAP", max_points=plot_max_points, seed=seed,
        )
        _write_coords(
            out_dir / "entities_umap.parquet", ent_meta[sel.tolist()], umap_coords
        )
        report["projection_quality"]["umap"] = M.projection_quality(
            entity_emb[sel], umap_coords, sample_size=metric_sample, seed=seed
        )

    # --- Relations ----------------------------------------------------------
    logger.info("Analyzing %d relation embeddings...", relation_emb.shape[0])
    report["relations"] = M.cluster_report(
        relation_emb, families, sample_size=metric_sample,
        knn_sample_size=knn_sample, knn_k=knn_k, seed=seed,
    )
    rel_pca, rel_ev = P.pca_project(relation_emb, seed=seed)
    P.plot_relation_scatter(
        rel_pca, relation_names, families, out_dir / f"relations_pca.{ext}",
        method="PCA", explained=rel_ev,
    )
    _write_coords(out_dir / "relations_pca.parquet", rel_meta, rel_pca)

    (out_dir / METRICS).write_text(json.dumps(report, indent=2))
    logger.info("Wrote cluster metrics to %s", out_dir / METRICS)
    _log_summary(report)


def _subsample_idx(n: int, sample_size: int, seed: int):
    """Return a sorted seeded subsample of row indices (or all rows)."""
    import numpy as np

    if n <= sample_size:
        return np.arange(n)
    rng = np.random.default_rng(seed)
    return np.sort(rng.choice(n, size=sample_size, replace=False))


def _write_coords(path: Path, meta, coords) -> None:
    """Write projection coordinates joined with their metadata."""
    import polars as pl

    meta.with_columns(
        pl.Series("x", coords[:, 0]), pl.Series("y", coords[:, 1])
    ).write_parquet(path)


def _log_summary(report: dict) -> None:
    """Log a compact human-readable summary of the headline metrics."""
    for scope in ("entities", "relations"):
        r = report.get(scope, {})
        if "silhouette_cosine" not in r:
            continue
        logger.info(
            "%-9s | silhouette=%.3f  DB=%.2f  CH=%.0f  kmeans-ARI=%.3f  NMI=%.3f%s",
            scope,
            r.get("silhouette_cosine", float("nan")),
            r.get("davies_bouldin", float("nan")),
            r.get("calinski_harabasz", float("nan")),
            r.get("kmeans_ari", float("nan")),
            r.get("kmeans_nmi", float("nan")),
            f"  kNN-F1={r['knn_macro_f1']:.3f}" if "knn_macro_f1" in r else "",
        )
    pq = report.get("projection_quality", {})
    for method, q in pq.items():
        logger.info(
            "%-9s | trustworthiness=%.3f  continuity=%.3f",
            method.upper(), q.get("trustworthiness", float("nan")),
            q.get("continuity", float("nan")),
        )
