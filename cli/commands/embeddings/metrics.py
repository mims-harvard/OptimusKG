"""Quantitative cluster-coherence metrics for embedding spaces.

The question "do entities/relations of the same type cluster coherently in
latent space?" is made quantitative here with metrics computed on the
**original high-dimensional embeddings**, never on the 2D PCA/UMAP projection.
This matters: UMAP/t-SNE preserve local neighbourhoods but distort global
inter-cluster distances, so a silhouette score read off a 2D map mostly
measures the projection, not the embedding. The 2D map is for the eye; the
numbers below are for the claim.

Three complementary families are reported, because each alone is gameable:

* **Internal indices** with the type labels treated as the clustering —
  silhouette (cosine), Davies-Bouldin and Calinski-Harabasz. They ask: are
  same-type points closer to each other than to other types?
* **Supervised separability** — a k-NN probe (cosine) measuring how well a
  point's type is predicted by its neighbours. Robust to non-convex, anisotropic
  clusters that silhouette penalises unfairly.
* **Unsupervised agreement** — k-means (k = #types) scored against the true
  labels with Adjusted Rand Index, Normalised Mutual Information and purity.
  Asks whether unsupervised structure rediscovers the types.

Separately, **trustworthiness** and **continuity** quantify how faithfully the
2D projection represents the high-D space, so the figure can be reported with a
caveat rather than as evidence.
"""

from __future__ import annotations

import logging

import numpy as np
from sklearn.cluster import KMeans
from sklearn.manifold import trustworthiness
from sklearn.metrics import (
    adjusted_rand_score,
    calinski_harabasz_score,
    davies_bouldin_score,
    f1_score,
    normalized_mutual_info_score,
    silhouette_samples,
    silhouette_score,
)
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import normalize

logger = logging.getLogger("cli")


def _encode(labels: list[str]) -> tuple[np.ndarray, list[str]]:
    """Encode string labels to contiguous ints; return ``(codes, classes)``."""
    classes, codes = np.unique(np.asarray(labels), return_inverse=True)
    return codes, classes.tolist()


def _subsample(n: int, sample_size: int | None, seed: int) -> np.ndarray:
    """Return row indices, subsampling to ``sample_size`` when ``n`` is larger."""
    if sample_size is None or n <= sample_size:
        return np.arange(n)
    rng = np.random.default_rng(seed)
    return np.sort(rng.choice(n, size=sample_size, replace=False))


def purity_score(labels_true: np.ndarray, labels_pred: np.ndarray) -> float:
    """Cluster purity: fraction of points in the majority class of their cluster."""
    total = 0
    for cluster in np.unique(labels_pred):
        members = labels_true[labels_pred == cluster]
        if members.size:
            total += int(np.bincount(members).max())
    return total / labels_true.shape[0]


def internal_indices(
    embeddings: np.ndarray,
    labels: list[str],
    *,
    sample_size: int | None = 10000,
    seed: int = 42,
) -> dict:
    """Silhouette (cosine, overall + per-type), Davies-Bouldin, Calinski-Harabasz.

    Silhouette is O(n^2), so it is computed on a seeded subsample of at most
    ``sample_size`` points. Davies-Bouldin and Calinski-Harabasz are cheap and
    use all points (on L2-normalised vectors, so Euclidean geometry matches the
    cosine geometry TransE embeddings live in).
    """
    codes, classes = _encode(labels)
    idx = _subsample(embeddings.shape[0], sample_size, seed)
    x_sample, y_sample = embeddings[idx], codes[idx]

    sil_values = silhouette_samples(x_sample, y_sample, metric="cosine")
    per_type = {
        classes[c]: float(sil_values[y_sample == c].mean())
        for c in np.unique(y_sample)
    }

    x_norm = normalize(embeddings)
    return {
        "silhouette_cosine": float(sil_values.mean()),
        "silhouette_per_type": per_type,
        "davies_bouldin": float(davies_bouldin_score(x_norm, codes)),
        "calinski_harabasz": float(calinski_harabasz_score(x_norm, codes)),
        "silhouette_sample_size": int(idx.shape[0]),
        "n_types": len(classes),
    }


def knn_probe(
    embeddings: np.ndarray,
    labels: list[str],
    *,
    k: int = 15,
    sample_size: int | None = 20000,
    test_fraction: float = 0.3,
    seed: int = 42,
) -> dict:
    """k-NN type-classification probe (cosine), reporting accuracy and macro-F1.

    A high score means a point's type is recoverable from its embedding
    neighbourhood — direct evidence that same-type entities are co-located,
    without silhouette's convex-cluster assumption.
    """
    codes, classes = _encode(labels)
    idx = _subsample(embeddings.shape[0], sample_size, seed)
    x, y = embeddings[idx], codes[idx]

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=test_fraction, stratify=y, random_state=seed
    )
    clf = KNeighborsClassifier(n_neighbors=k, metric="cosine")
    clf.fit(x_train, y_train)
    y_pred = clf.predict(x_test)

    return {
        "knn_accuracy": float((y_pred == y_test).mean()),
        "knn_macro_f1": float(f1_score(y_test, y_pred, average="macro")),
        "knn_k": k,
        "knn_eval_size": int(x_test.shape[0]),
        "n_types": len(classes),
    }


def kmeans_agreement(
    embeddings: np.ndarray,
    labels: list[str],
    *,
    sample_size: int | None = 20000,
    seed: int = 42,
) -> dict:
    """k-means (k = #types) vs true labels: Adjusted Rand, NMI, purity.

    Tests whether *unsupervised* structure in the embedding space rediscovers
    the known types. Computed on L2-normalised vectors (spherical k-means).
    """
    codes, classes = _encode(labels)
    idx = _subsample(embeddings.shape[0], sample_size, seed)
    x, y = normalize(embeddings[idx]), codes[idx]

    pred = KMeans(n_clusters=len(classes), random_state=seed, n_init=10).fit_predict(x)
    return {
        "kmeans_ari": float(adjusted_rand_score(y, pred)),
        "kmeans_nmi": float(normalized_mutual_info_score(y, pred)),
        "kmeans_purity": purity_score(y, pred),
        "kmeans_eval_size": int(x.shape[0]),
        "n_types": len(classes),
    }


def projection_quality(
    high: np.ndarray,
    low: np.ndarray,
    *,
    n_neighbors: int = 15,
    sample_size: int | None = 10000,
    seed: int = 42,
) -> dict:
    """Trustworthiness and continuity of a 2D projection vs the high-D space.

    Trustworthiness penalises false neighbours introduced by the projection;
    continuity penalises true neighbours it tears apart. Both lie in ``[0, 1]``
    (1 = faithful). Continuity is trustworthiness with the spaces swapped.
    """
    idx = _subsample(high.shape[0], sample_size, seed)
    h, lo = high[idx], low[idx]
    return {
        "trustworthiness": float(trustworthiness(h, lo, n_neighbors=n_neighbors)),
        "continuity": float(trustworthiness(lo, h, n_neighbors=n_neighbors)),
        "projection_n_neighbors": n_neighbors,
        "projection_sample_size": int(idx.shape[0]),
    }


def cluster_report(
    embeddings: np.ndarray,
    labels: list[str],
    *,
    sample_size: int | None = 10000,
    knn_sample_size: int | None = 20000,
    knn_k: int = 15,
    seed: int = 42,
) -> dict:
    """Run the full internal + supervised + unsupervised metric suite.

    Returns a flat-ish dict suitable for JSON serialisation. The k-NN probe and
    k-means agreement are skipped (with a logged note) when there are fewer than
    two types or too few points to split.
    """
    n, n_types = embeddings.shape[0], len(set(labels))
    report: dict = {"n_points": int(n), "n_types": n_types}
    if n_types < 2:
        logger.warning("Only %d type(s); cluster metrics need >= 2.", n_types)
        return report

    report.update(internal_indices(embeddings, labels, sample_size=sample_size, seed=seed))
    report.update(
        kmeans_agreement(embeddings, labels, sample_size=knn_sample_size, seed=seed)
    )
    # The k-NN probe needs a stratified split, so every class must have >= 2
    # members (skipped for e.g. relation families with a single relation).
    min_class = int(np.bincount(_encode(labels)[0]).min())
    if n > 2 * n_types and min_class >= 2:
        report.update(
            knn_probe(
                embeddings,
                labels,
                k=min(knn_k, n // (2 * n_types)),
                sample_size=knn_sample_size,
                seed=seed,
            )
        )
    else:
        logger.info("Skipping k-NN probe (min class size %d < 2 or too few points).", min_class)
    return report
