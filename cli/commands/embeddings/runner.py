"""Orchestration for the embeddings sub-commands.

The typer wrappers in :mod:`__init__` delegate here. Heavy dependencies
(numpy, torch, scikit-learn, matplotlib, umap) are imported at module load, so
this module is itself imported lazily from the command bodies — importing the
sub-app alone never pulls them in.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np
import polars as pl

from . import metrics as cluster_metrics
from . import project as proj
from .data import (
    DEFAULT_DOI,
    Triples,
    ensure_gold_data,
    load_triples,
    relation_family,
)
from .transe import TransEConfig, train_transe

logger = logging.getLogger("cli")

# Artifact filenames written by `train` and read by `analyze`.
ENTITY_EMB = "entity_emb.npy"
RELATION_EMB = "relation_emb.npy"
ENTITIES = "entities.parquet"
RELATIONS = "relations.parquet"
TRAIN_LOG = "train_log.json"
METRICS = "cluster_metrics.json"

VALID_FORMATS = ("pdf", "svg")


def download_gold(
    nodes_path: Path,
    edges_path: Path,
    *,
    lcc: bool,
    cache_dir: Path | None,
    doi: str | None,
) -> None:
    """Download the gold graph via the optimuskg client (see :func:`ensure_gold_data`)."""
    ensure_gold_data(
        nodes_path,
        edges_path,
        lcc=lcc,
        download=True,
        cache_dir=cache_dir,
        doi=doi or DEFAULT_DOI,
    )


def train_embeddings(  # noqa: PLR0913
    nodes_path: Path,
    edges_path: Path,
    out_dir: Path,
    *,
    relation_key: str,
    lcc: bool,
    download: bool,
    max_edges: int | None,
    config: TransEConfig,
) -> None:
    """Load triples, train TransE, and persist embeddings + metadata to ``out_dir``.

    ``config`` carries all TransE hyperparameters (see :class:`TransEConfig`).
    """
    nodes_path, edges_path = ensure_gold_data(
        nodes_path, edges_path, lcc=lcc, download=download
    )
    triples = load_triples(
        nodes_path,
        edges_path,
        relation_key=relation_key,
        max_edges=max_edges,
        seed=config.seed,
    )
    entity_emb, relation_emb, log = train_transe(
        triples.head,
        triples.relation,
        triples.tail,
        triples.n_entities,
        triples.n_relations,
        config,
    )
    log["relation_key"] = relation_key
    log["lcc"] = lcc
    _save_artifacts(out_dir, triples, entity_emb, relation_emb, log)
    logger.info("Wrote embedding artifacts to %s", out_dir)


def analyze_embeddings(  # noqa: PLR0913
    out_dir: Path,
    *,
    fmt: str,
    do_umap: bool,
    umap_sample: int,
    umap_params: proj.UmapParams | None = None,
    plot_max_points: int,
    metric_sample: int,
    knn_sample: int,
    knn_k: int,
    seed: int,
) -> None:
    """Project embeddings (PCA/UMAP), render scatters, and compute cluster metrics.

    Metrics are computed on the full-dimensional embeddings; the 2D projections
    are scored with trustworthiness / continuity so the figure can be reported
    with its faithfulness stated rather than assumed. ``umap_params`` controls the
    UMAP projection (n_neighbors, min_dist, metric).
    """
    if fmt.lower() not in VALID_FORMATS:
        msg = f"--format must be one of {VALID_FORMATS}, got {fmt!r}"
        raise ValueError(msg)
    ext = fmt.lower()
    umap_params = umap_params or proj.UmapParams()

    # float64 avoids float32 overflow/invalid warnings in sklearn's matmul-based
    # routines (randomized SVD, pairwise distances) and stabilises PCA.
    entity_emb = np.load(out_dir / ENTITY_EMB).astype(np.float64)
    relation_emb = np.load(out_dir / RELATION_EMB).astype(np.float64)
    ent_meta = pl.read_parquet(out_dir / ENTITIES)
    rel_meta = pl.read_parquet(out_dir / RELATIONS)
    types = ent_meta["type"].to_list()
    relation_names = rel_meta["relation"].to_list()
    # Recompute families from the relation names so relabelings in
    # RELATION_FAMILY_GROUPS take effect without retraining.
    families = [relation_family(r) for r in relation_names]
    rel_meta = rel_meta.with_columns(pl.Series("family", families))

    report: dict = {}
    report["entities"] = _analyze_entities(
        out_dir,
        ext,
        entity_emb,
        ent_meta,
        types,
        do_umap=do_umap,
        umap_sample=umap_sample,
        umap_params=umap_params,
        plot_max_points=plot_max_points,
        metric_sample=metric_sample,
        knn_sample=knn_sample,
        knn_k=knn_k,
        seed=seed,
    )
    report["projection_quality"] = report["entities"].pop("_projection_quality")
    report["relations"] = _analyze_relations(
        out_dir,
        ext,
        relation_emb,
        rel_meta,
        relation_names,
        families,
        do_umap=do_umap,
        umap_params=umap_params,
        metric_sample=metric_sample,
        knn_sample=knn_sample,
        knn_k=knn_k,
        seed=seed,
    )

    if do_umap:
        _build_combined_figure(
            out_dir, ext, report["entities"].get("silhouette_per_type", {})
        )

    (out_dir / METRICS).write_text(json.dumps(report, indent=2))
    logger.info("Wrote cluster metrics to %s", out_dir / METRICS)
    _log_summary(report)


def _build_combined_figure(out_dir: Path, ext: str, silhouette_per_type: dict) -> None:
    """Assemble the multi-panel figure from the saved UMAP/PCA coordinate tables."""
    ent = pl.read_parquet(out_dir / "entities_umap.parquet")
    rel = pl.read_parquet(out_dir / "relations_pca.parquet")
    proj.plot_combined_figure(
        ent.select("x", "y").to_numpy(),
        ent["type"].to_list(),
        rel.select("x", "y").to_numpy(),
        rel["relation"].to_list(),
        rel["family"].to_list(),
        silhouette_per_type,
        out_dir / f"combined.{ext}",
    )


def _analyze_entities(  # noqa: PLR0913
    out_dir: Path,
    ext: str,
    entity_emb: np.ndarray,
    ent_meta: pl.DataFrame,
    types: list[str],
    *,
    do_umap: bool,
    umap_sample: int,
    umap_params: proj.UmapParams,
    plot_max_points: int,
    metric_sample: int,
    knn_sample: int,
    knn_k: int,
    seed: int,
) -> dict:
    """Cluster metrics + PCA/UMAP scatters for entity embeddings."""
    logger.info("Analyzing %d entity embeddings...", entity_emb.shape[0])
    metrics = cluster_metrics.cluster_report(
        entity_emb,
        types,
        sample_size=metric_sample,
        knn_sample_size=knn_sample,
        knn_k=knn_k,
        seed=seed,
    )

    pca_coords, ev = proj.pca_project(entity_emb, seed=seed)
    proj.plot_entity_scatter(
        pca_coords,
        types,
        out_dir / f"entities_pca.{ext}",
        method="PCA",
        explained=ev,
        max_points=plot_max_points,
        seed=seed,
    )
    _write_coords(out_dir / "entities_pca.parquet", ent_meta, pca_coords)
    projection_quality = {
        "pca": cluster_metrics.projection_quality(
            entity_emb, pca_coords, sample_size=metric_sample, seed=seed
        )
    }
    if "silhouette_per_type" in metrics:
        proj.plot_silhouette_bar(
            metrics["silhouette_per_type"], out_dir / f"silhouette_by_type.{ext}"
        )

    if do_umap:
        sel = _subsample_idx(entity_emb.shape[0], umap_sample, seed)
        logger.info("Computing UMAP on %d entities...", sel.shape[0])
        umap_coords = proj.umap_project(entity_emb[sel], seed=seed, params=umap_params)
        proj.plot_entity_scatter(
            umap_coords,
            [types[i] for i in sel],
            out_dir / f"entities_umap.{ext}",
            method="UMAP",
            max_points=plot_max_points,
            seed=seed,
        )
        _write_coords(
            out_dir / "entities_umap.parquet", _select_rows(ent_meta, sel), umap_coords
        )
        projection_quality["umap"] = cluster_metrics.projection_quality(
            entity_emb[sel], umap_coords, sample_size=metric_sample, seed=seed
        )

    metrics["_projection_quality"] = projection_quality
    return metrics


def _analyze_relations(  # noqa: PLR0913
    out_dir: Path,
    ext: str,
    relation_emb: np.ndarray,
    rel_meta: pl.DataFrame,
    relation_names: list[str],
    families: list[str],
    *,
    do_umap: bool,
    umap_params: proj.UmapParams,
    metric_sample: int,
    knn_sample: int,
    knn_k: int,
    seed: int,
) -> dict:
    """Cluster metrics (by family) + labelled PCA/UMAP scatters for relations."""
    logger.info("Analyzing %d relation embeddings...", relation_emb.shape[0])
    metrics = cluster_metrics.cluster_report(
        relation_emb,
        families,
        sample_size=metric_sample,
        knn_sample_size=knn_sample,
        knn_k=knn_k,
        seed=seed,
    )
    rel_pca, rel_ev = proj.pca_project(relation_emb, seed=seed)
    proj.plot_relation_scatter(
        rel_pca,
        relation_names,
        families,
        out_dir / f"relations_pca.{ext}",
        method="PCA",
        explained=rel_ev,
    )
    _write_coords(out_dir / "relations_pca.parquet", rel_meta, rel_pca)

    if do_umap:
        rel_umap = proj.umap_project(relation_emb, seed=seed, params=umap_params)
        proj.plot_relation_scatter(
            rel_umap,
            relation_names,
            families,
            out_dir / f"relations_umap.{ext}",
            method="UMAP",
        )
        _write_coords(out_dir / "relations_umap.parquet", rel_meta, rel_umap)
    return metrics


def _save_artifacts(
    out_dir: Path,
    triples: Triples,
    entity_emb: np.ndarray,
    relation_emb: np.ndarray,
    log: dict,
) -> None:
    """Persist embeddings, aligned metadata, and the training log."""
    out_dir.mkdir(parents=True, exist_ok=True)
    np.save(out_dir / ENTITY_EMB, entity_emb)
    np.save(out_dir / RELATION_EMB, relation_emb)
    pl.DataFrame(
        {"id": triples.entity_ids, "type": triples.entity_types}
    ).write_parquet(out_dir / ENTITIES)
    pl.DataFrame(
        {
            "relation": triples.relation_names,
            "family": [relation_family(r) for r in triples.relation_names],
        }
    ).write_parquet(out_dir / RELATIONS)
    (out_dir / TRAIN_LOG).write_text(json.dumps(log, indent=2))


def _subsample_idx(n: int, sample_size: int, seed: int) -> np.ndarray:
    """Return a sorted seeded subsample of row indices (or all rows)."""
    if n <= sample_size:
        return np.arange(n)
    rng = np.random.default_rng(seed)
    return np.sort(rng.choice(n, size=sample_size, replace=False))


def _select_rows(df: pl.DataFrame, idx: np.ndarray) -> pl.DataFrame:
    """Return ``df`` rows at the (sorted, ascending) positional indices ``idx``."""
    return (
        df.with_row_index("__i").filter(pl.col("__i").is_in(idx.tolist())).drop("__i")
    )


def _write_coords(path: Path, meta: pl.DataFrame, coords: np.ndarray) -> None:
    """Write projection coordinates joined with their metadata."""
    meta.with_columns(
        pl.Series("x", coords[:, 0]), pl.Series("y", coords[:, 1])
    ).write_parquet(path)


def _log_summary(report: dict) -> None:
    """Log a compact human-readable summary of the headline metrics."""
    for scope in ("entities", "relations"):
        r = report.get(scope, {})
        if "silhouette_euclidean" not in r:
            continue
        probes = (
            f"  kNN-F1={r['knn_macro_f1']:.3f}"
            f"  lin-F1={r.get('linear_probe_macro_f1', float('nan')):.3f}"
            if "knn_macro_f1" in r
            else ""
        )
        logger.info(
            "%-9s | sil(eucl)=%.3f  DB=%.2f  CH=%.0f  ARI=%.3f  AMI=%.3f%s",
            scope,
            r.get("silhouette_euclidean", float("nan")),
            r.get("davies_bouldin", float("nan")),
            r.get("calinski_harabasz", float("nan")),
            r.get("kmeans_ari", float("nan")),
            r.get("kmeans_ami", float("nan")),
            probes,
        )
    for method, q in report.get("projection_quality", {}).items():
        logger.info(
            "%-9s | trustworthiness=%.3f  continuity=%.3f",
            method.upper(),
            q.get("trustworthiness", float("nan")),
            q.get("continuity", float("nan")),
        )
