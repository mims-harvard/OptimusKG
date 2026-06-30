"""Dimensionality reduction (PCA / UMAP) and scatter plots of embeddings.

Projections are used purely for *visualisation*; the quantitative claims live in
:mod:`cli.commands.embeddings.metrics`, computed on the full-dimensional
embeddings. Entity scatters are rasterised and optionally subsampled to keep
vector output small; relation scatters are labelled with their relation name.
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D
from sklearn.decomposition import PCA

# Importing the figures style module registers the shared "mpll" matplotlib
# style and the project palette/helpers.
from cli.commands.figures.style import STYLE, apply_axis_styling, apply_legend_styling

from .data import NODE_TYPE_NAME, categorical_colors

logger = logging.getLogger("cli")

# Projections are always to 2 dimensions.
_PLOT_DIMS = 2

# STYLE holds heterogeneously-typed values; pin the few passed to matplotlib
# APIs with strict numeric signatures (mirrors the cli/commands/figures pattern).
_TITLE_PAD: float = STYLE["title_pad"]  # ty: ignore[invalid-assignment]
_TIGHT_RECT: tuple[float, float, float, float] = STYLE["fig_tight_rect"]  # ty: ignore[invalid-assignment]


def pca_project(
    embeddings: np.ndarray, *, seed: int = 42
) -> tuple[np.ndarray, list[float]]:
    """Project to 2D with PCA; return ``(coords, explained_variance_ratio)``."""
    pca = PCA(n_components=2, random_state=seed)
    coords = pca.fit_transform(embeddings)
    return coords, pca.explained_variance_ratio_.tolist()


def umap_project(
    embeddings: np.ndarray,
    *,
    seed: int = 42,
    n_neighbors: int = 15,
    min_dist: float = 0.1,
) -> np.ndarray:
    """Project to 2D with UMAP (imported lazily — it is an optional dependency)."""
    import umap  # noqa: PLC0415 — optional heavy dependency, import on demand

    n_neighbors = min(n_neighbors, max(2, embeddings.shape[0] - 1))
    reducer = umap.UMAP(
        n_components=2,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric="cosine",
        random_state=seed,
    )
    # UMAP's float32 spectral initialisation can emit benign overflow/invalid
    # RuntimeWarnings from its internal matmuls; silence just those.
    with warnings.catch_warnings(), np.errstate(over="ignore", invalid="ignore"):
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return reducer.fit_transform(embeddings)


def _legend_handles(color_map: dict[str, str]) -> list[Line2D]:
    """Build round marker handles for a category->colour map."""
    return [
        Line2D(
            [],
            [],
            marker="o",
            linestyle="",
            markersize=4,
            markerfacecolor=color,
            markeredgecolor="none",
            label=label,
        )
        for label, color in color_map.items()
    ]


def plot_entity_scatter(  # noqa: PLR0913
    coords: np.ndarray,
    types: list[str],
    out_path: Path,
    *,
    method: str,
    explained: list[float] | None = None,
    max_points: int = 60000,
    seed: int = 42,
) -> None:
    """Scatter entity projections coloured by node type.

    Args:
        coords: ``(n, 2)`` projected coordinates.
        types: Node type code per row (e.g. ``"GEN"``).
        out_path: Output figure path (``.pdf`` or ``.svg``).
        method: Projection name for axis labels (``"PCA"`` / ``"UMAP"``).
        explained: PCA explained-variance ratios, for axis labels.
        max_points: Cap on plotted points (seeded subsample) to bound file size.
        seed: Subsampling seed.
    """
    types_arr = np.asarray(types)
    if coords.shape[0] > max_points:
        rng = np.random.default_rng(seed)
        sel = rng.choice(coords.shape[0], size=max_points, replace=False)
        coords, types_arr = coords[sel], types_arr[sel]

    ordered = [t for t in NODE_TYPE_NAME if t in set(types_arr.tolist())]
    color_map = categorical_colors(ordered)

    fig, ax = plt.subplots(figsize=(5.0, 4.2))
    # Plot largest types first so smaller ones land on top and stay visible.
    for node_type in sorted(ordered, key=lambda t: -(types_arr == t).sum()):
        mask = types_arr == node_type
        ax.scatter(
            coords[mask, 0],
            coords[mask, 1],
            s=3,
            alpha=0.45,
            linewidths=0,
            color=color_map[node_type],
            rasterized=True,
        )

    _label_axes(ax, method, explained)
    ax.set_title(
        f"TransE entity embeddings ({method})",
        fontsize=STYLE["title_fontsize"],
        fontweight=STYLE["title_fontweight"],
        pad=_TITLE_PAD,
    )
    apply_axis_styling(ax)
    legend = ax.legend(
        handles=_legend_handles({NODE_TYPE_NAME[t]: color_map[t] for t in ordered}),
        title="Node type",
        loc="best",
        fontsize=STYLE["legend_fontsize"],
        title_fontsize=STYLE["legend_fontsize"],
        frameon=STYLE["legend_frameon"],
        framealpha=STYLE["legend_framealpha"],
        edgecolor=STYLE["legend_edgecolor"],
        ncols=1,
        markerscale=1.5,
    )
    apply_legend_styling(legend)
    _save(fig, out_path)


def plot_relation_scatter(  # noqa: PLR0913
    coords: np.ndarray,
    relation_names: list[str],
    families: list[str],
    out_path: Path,
    *,
    method: str,
    explained: list[float] | None = None,
) -> None:
    """Scatter relation projections coloured by semantic family, with labels."""
    fams_arr = np.asarray(families)
    ordered_fams = sorted(set(families))
    color_map = categorical_colors(ordered_fams)

    fig, ax = plt.subplots(figsize=(5.6, 4.6))
    for family in ordered_fams:
        mask = fams_arr == family
        ax.scatter(
            coords[mask, 0],
            coords[mask, 1],
            s=45,
            alpha=0.9,
            linewidths=0.4,
            edgecolors="white",
            color=color_map[family],
            label=family,
        )

    _annotate_points(ax, coords, relation_names)
    _label_axes(ax, method, explained)
    ax.set_title(
        f"TransE relation embeddings ({method})",
        fontsize=STYLE["title_fontsize"],
        fontweight=STYLE["title_fontweight"],
        pad=_TITLE_PAD,
    )
    apply_axis_styling(ax)
    legend = ax.legend(
        title="Relation family",
        loc="best",
        fontsize=STYLE["legend_fontsize"],
        title_fontsize=STYLE["legend_fontsize"],
        frameon=STYLE["legend_frameon"],
        framealpha=STYLE["legend_framealpha"],
        edgecolor=STYLE["legend_edgecolor"],
    )
    apply_legend_styling(legend)
    _save(fig, out_path)


def _annotate_points(ax: plt.Axes, coords: np.ndarray, names: list[str]) -> None:
    """Label scatter points, using adjustText to reduce overlap when available."""
    texts = [
        ax.text(x, y, name, fontsize=4.5, ha="center", va="center")
        for (x, y), name in zip(coords, names)
    ]
    try:
        from adjustText import adjust_text  # noqa: PLC0415

        adjust_text(
            texts,
            ax=ax,
            expand=(1.2, 1.4),
            arrowprops={"arrowstyle": "-", "lw": 0.3, "color": "0.6"},
        )
    except ImportError:
        logger.debug("adjustText unavailable; labels may overlap.")


def _label_axes(ax: plt.Axes, method: str, explained: list[float] | None) -> None:
    """Set projection axis labels, including PCA variance when provided."""
    if explained is not None and len(explained) >= _PLOT_DIMS:
        ax.set_xlabel(
            f"{method} 1 ({explained[0] * 100:.1f}%)",
            fontsize=STYLE["axis_label_fontsize"],
        )
        ax.set_ylabel(
            f"{method} 2 ({explained[1] * 100:.1f}%)",
            fontsize=STYLE["axis_label_fontsize"],
        )
    else:
        ax.set_xlabel(f"{method} 1", fontsize=STYLE["axis_label_fontsize"])
        ax.set_ylabel(f"{method} 2", fontsize=STYLE["axis_label_fontsize"])


def plot_silhouette_bar(per_type: dict[str, float], out_path: Path) -> None:
    """Horizontal bar chart of per-type mean silhouette (cosine)."""
    items = sorted(per_type.items(), key=lambda kv: kv[1])
    names = [NODE_TYPE_NAME.get(t, t) for t, _ in items]
    values = [v for _, v in items]
    color_map = categorical_colors([t for t, _ in items])

    fig, ax = plt.subplots(figsize=(4.6, 0.32 * len(items) + 1.0))
    ax.barh(
        names,
        values,
        color=[color_map[t] for t, _ in items],
        edgecolor=STYLE["bar_edgecolor"],
        linewidth=STYLE["bar_linewidth"],
        alpha=STYLE["bar_alpha"],
    )
    ax.axvline(0, color=STYLE["bar_edgecolor"], linewidth=0.5)
    ax.set_xlabel("Mean silhouette (cosine)", fontsize=STYLE["axis_label_fontsize"])
    ax.set_title(
        "Per-type cluster coherence",
        fontsize=STYLE["title_fontsize"],
        fontweight=STYLE["title_fontweight"],
        pad=_TITLE_PAD,
    )
    apply_axis_styling(ax)
    _save(fig, out_path)


def _save(fig: plt.Figure, out_path: Path) -> None:
    """Tight-layout, save at configured DPI, and close the figure."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(rect=_TIGHT_RECT)
    fig.savefig(
        out_path,
        dpi=STYLE["fig_dpi"],
        bbox_inches="tight",
        facecolor=STYLE["fig_facecolor"],
    )
    plt.close(fig)
    logger.info("Wrote %s", out_path)
