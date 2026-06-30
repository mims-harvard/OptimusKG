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
  silhouette, Davies-Bouldin and Calinski-Harabasz. They ask: are same-type
  points closer to each other than to other types? Distances are **Euclidean**,
  because TransE scores triples by the translation distance ``||h + r - t||``
  (L1/L2), so the embedding lives in a Euclidean geometry; silhouette is also
  reported with cosine for reference. These indices assume convex, roughly
  isotropic clusters, so they are read as relative, not absolute, evidence.
* **Supervised separability** — a k-NN probe and a logistic-regression linear
  probe (both Euclidean) measuring how well a point's type is predicted from its
  neighbourhood / by a hyperplane. Make no cluster-shape assumption, so they are
  the most direct evidence that type is recoverable.
* **Unsupervised agreement** — k-means (k = #types) scored against the true
  labels with Adjusted Rand Index, Adjusted Mutual Information (both chance-
  corrected and shape-agnostic), NMI and purity. Asks whether unsupervised
  structure rediscovers the types.

Separately, **trustworthiness** and **continuity** quantify how faithfully the
2D projection represents the high-D space, so the figure can be reported with a
caveat rather than as evidence.
"""

from __future__ import annotations

import logging

import numpy as np
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression
from sklearn.manifold import trustworthiness
from sklearn.metrics import (
    adjusted_mutual_info_score,
    adjusted_rand_score,
    calinski_harabasz_score,
    davies_bouldin_score,
    f1_score,
    normalized_mutual_info_score,
    silhouette_samples,
)
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier

logger = logging.getLogger("cli")

# A clustering needs at least two classes; a stratified split and a sensible
# k-NN probe need at least this many members per class / points per type.
_MIN_TYPES = 2
_MIN_CLASS_SIZE = 2
_MIN_POINTS_PER_TYPE = 2


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
    """Silhouette (Euclidean + cosine), Davies-Bouldin, Calinski-Harabasz.

    TransE scores triples by the translation distance ``||h + r - t||`` (L1/L2),
    so its native geometry is **Euclidean** — silhouette is therefore reported
    primarily with Euclidean distance, with cosine alongside for reference
    (entity vectors are unit-normalised by the training constraint, so the two
    nearly coincide for entities but differ for the unnormalised relations).
    Davies-Bouldin and Calinski-Harabasz are inherently Euclidean and computed
    on the raw embeddings (all points). Silhouette is O(n^2), so it uses a
    seeded subsample of at most ``sample_size`` points.
    """
    codes, classes = _encode(labels)
    idx = _subsample(embeddings.shape[0], sample_size, seed)
    x_sample, y_sample = embeddings[idx], codes[idx]

    sil_euclidean = silhouette_samples(x_sample, y_sample, metric="euclidean")
    sil_cosine = silhouette_samples(x_sample, y_sample, metric="cosine")
    per_type = {
        classes[c]: float(sil_euclidean[y_sample == c].mean())
        for c in np.unique(y_sample)
    }

    return {
        "silhouette_euclidean": float(sil_euclidean.mean()),
        "silhouette_cosine": float(sil_cosine.mean()),
        "silhouette_per_type": per_type,
        "davies_bouldin": float(davies_bouldin_score(embeddings, codes)),
        "calinski_harabasz": float(calinski_harabasz_score(embeddings, codes)),
        "silhouette_sample_size": int(idx.shape[0]),
        "n_types": len(classes),
    }


def knn_probe(  # noqa: PLR0913
    embeddings: np.ndarray,
    labels: list[str],
    *,
    k: int = 15,
    sample_size: int | None = 20000,
    test_fraction: float = 0.3,
    seed: int = 42,
) -> dict:
    """k-NN type-classification probe (Euclidean), reporting accuracy and macro-F1.

    A high score means a point's type is recoverable from its embedding
    neighbourhood — direct evidence that same-type entities are co-located,
    without silhouette's convex-cluster assumption. Euclidean distance matches
    TransE's translational geometry; macro-F1 guards against type imbalance.
    """
    codes, classes = _encode(labels)
    idx = _subsample(embeddings.shape[0], sample_size, seed)
    x, y = embeddings[idx], codes[idx]

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=test_fraction, stratify=y, random_state=seed
    )
    clf = KNeighborsClassifier(n_neighbors=k, metric="euclidean")
    clf.fit(x_train, y_train)
    y_pred = clf.predict(x_test)

    return {
        "knn_accuracy": float((y_pred == y_test).mean()),
        "knn_macro_f1": float(f1_score(y_test, y_pred, average="macro")),
        "knn_k": k,
        "knn_eval_size": int(x_test.shape[0]),
        "n_types": len(classes),
    }


def linear_probe(
    embeddings: np.ndarray,
    labels: list[str],
    *,
    sample_size: int | None = 20000,
    test_fraction: float = 0.3,
    seed: int = 42,
) -> dict:
    """Logistic-regression linear probe: is type *linearly* separable in the space?

    Complementary to the k-NN probe — k-NN measures local neighbourhood
    coherence, the linear probe measures whether a single hyperplane per type
    suffices. Standard frozen-feature evaluation protocol; macro-F1 reported for
    type imbalance.
    """
    codes, classes = _encode(labels)
    idx = _subsample(embeddings.shape[0], sample_size, seed)
    x, y = embeddings[idx], codes[idx]

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=test_fraction, stratify=y, random_state=seed
    )
    clf = LogisticRegression(max_iter=1000, n_jobs=-1)
    clf.fit(x_train, y_train)
    y_pred = clf.predict(x_test)

    return {
        "linear_probe_accuracy": float((y_pred == y_test).mean()),
        "linear_probe_macro_f1": float(f1_score(y_test, y_pred, average="macro")),
        "linear_probe_eval_size": int(x_test.shape[0]),
        "n_types": len(classes),
    }


def kmeans_agreement(
    embeddings: np.ndarray,
    labels: list[str],
    *,
    sample_size: int | None = 20000,
    seed: int = 42,
) -> dict:
    """k-means (k = #types) vs true labels: Adjusted Rand, AMI, NMI, purity.

    Tests whether *unsupervised* structure rediscovers the known types. ARI and
    AMI are chance-corrected and make no cluster-shape assumption (preferred over
    raw NMI). k-means is Euclidean, matching TransE's geometry, and is run on the
    raw embeddings.
    """
    codes, classes = _encode(labels)
    idx = _subsample(embeddings.shape[0], sample_size, seed)
    x, y = embeddings[idx], codes[idx]

    pred = KMeans(n_clusters=len(classes), random_state=seed, n_init=10).fit_predict(x)
    return {
        "kmeans_ari": float(adjusted_rand_score(y, pred)),
        "kmeans_ami": float(adjusted_mutual_info_score(y, pred)),
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


def cluster_report(  # noqa: PLR0913
    embeddings: np.ndarray,
    labels: list[str],
    *,
    sample_size: int | None = 10000,
    knn_sample_size: int | None = 20000,
    knn_k: int = 15,
    seed: int = 42,
) -> dict:
    """Run the full internal + supervised + unsupervised metric suite.

    Returns a flat-ish dict suitable for JSON serialisation. The supervised
    probes (k-NN and linear) are skipped (with a logged note) when there are
    fewer than two types or too few points for a stratified split.
    """
    n, n_types = embeddings.shape[0], len(set(labels))
    report: dict = {"n_points": int(n), "n_types": n_types}
    if n_types < _MIN_TYPES:
        logger.warning("Only %d type(s); cluster metrics need >= 2.", n_types)
        return report

    report.update(
        internal_indices(embeddings, labels, sample_size=sample_size, seed=seed)
    )
    report.update(
        kmeans_agreement(embeddings, labels, sample_size=knn_sample_size, seed=seed)
    )
    # The supervised probes need a stratified split, so every class must have at
    # least _MIN_CLASS_SIZE members (skipped for e.g. single-relation families).
    min_class = int(np.bincount(_encode(labels)[0]).min())
    if n > _MIN_POINTS_PER_TYPE * n_types and min_class >= _MIN_CLASS_SIZE:
        report.update(
            knn_probe(
                embeddings,
                labels,
                k=min(knn_k, n // (_MIN_POINTS_PER_TYPE * n_types)),
                sample_size=knn_sample_size,
                seed=seed,
            )
        )
        report.update(
            linear_probe(embeddings, labels, sample_size=knn_sample_size, seed=seed)
        )
    else:
        logger.info(
            "Skipping supervised probes (min class size %d too small or too few "
            "points).",
            min_class,
        )
    return report
