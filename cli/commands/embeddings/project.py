"""Dimensionality reduction (PCA / UMAP) and scatter plots of embeddings.

Projections are used purely for *visualisation*; the quantitative claims live in
:mod:`cli.commands.embeddings.metrics`, computed on the full-dimensional
embeddings. Entity scatters are rasterised and optionally subsampled to keep
vector output small; relation scatters are labelled with their relation name.
"""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D
from sklearn.decomposition import PCA

# Importing the figures style module registers the shared "mpll" matplotlib
# style and the project palette/helpers.
from cli.commands.figures.style import STYLE, apply_axis_styling

from .data import NODE_TYPE_NAME, categorical_colors, node_type_colors

logger = logging.getLogger("cli")

# Projections are always to 2 dimensions.
_PLOT_DIMS = 2

# STYLE holds heterogeneously-typed values; pin the one passed to a matplotlib
# API with a strict numeric signature (mirrors the cli/commands/figures pattern).
_TIGHT_RECT: tuple[float, float, float, float] = STYLE["fig_tight_rect"]  # ty: ignore[invalid-assignment]


def pca_project(
    embeddings: np.ndarray, *, seed: int = 42
) -> tuple[np.ndarray, list[float]]:
    """Project to 2D with PCA; return ``(coords, explained_variance_ratio)``."""
    pca = PCA(n_components=2, random_state=seed)
    coords = pca.fit_transform(embeddings)
    return coords, pca.explained_variance_ratio_.tolist()


@dataclass
class UmapParams:
    """UMAP projection hyperparameters.

    Attributes:
        n_neighbors: Local-neighbourhood size; smaller emphasises fine local
            structure, larger preserves more global structure (closes gaps).
        min_dist: Minimum spacing of points within a cluster; larger loosens and
            rounds clusters (less dense). Must be <= ``spread``.
        spread: Overall scale of the embedding; raise it (together with
            ``min_dist``) to spread clusters further than ``min_dist`` alone
            allows.
        metric: Distance metric. ``"cosine"`` and ``"euclidean"`` coincide for
            the unit-normalised entity embeddings; either is fine.
    """

    n_neighbors: int = 30
    min_dist: float = 0.9
    spread: float = 1.5
    metric: str = "cosine"


def umap_project(
    embeddings: np.ndarray,
    *,
    seed: int = 42,
    params: UmapParams | None = None,
) -> np.ndarray:
    """Project to 2D with UMAP (imported lazily — it is an optional dependency)."""
    import umap  # noqa: PLC0415 — optional heavy dependency, import on demand

    params = params or UmapParams()
    n_neighbors = min(params.n_neighbors, max(2, embeddings.shape[0] - 1))
    reducer = umap.UMAP(
        n_components=2,
        n_neighbors=n_neighbors,
        min_dist=params.min_dist,
        spread=params.spread,
        metric=params.metric,
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


def _legend_below(
    ax: plt.Axes,
    *,
    title: str,
    ncols: int,
    handles: list[Line2D] | None = None,
    markerscale: float = 1.0,
) -> None:
    """Place a frameless legend in a horizontal row centred below the axes."""
    kwargs = {"handles": handles} if handles is not None else {}
    ax.legend(
        **kwargs,
        title=title,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.16),
        ncols=ncols,
        frameon=False,
        fontsize=STYLE["legend_fontsize"],
        title_fontsize=STYLE["legend_fontsize"],
        markerscale=markerscale,
    )


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

    color_map = node_type_colors(types_arr.tolist())
    ordered = list(color_map)

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
    apply_axis_styling(ax)
    _legend_below(
        ax,
        title="Node type",
        ncols=4,
        handles=_legend_handles({NODE_TYPE_NAME[t]: color_map[t] for t in ordered}),
        markerscale=1.5,
    )
    _save(fig, out_path)


def plot_relation_scatter(  # noqa: PLR0913
    coords: np.ndarray,
    relation_names: list[str],
    families: list[str],
    out_path: Path,
    *,
    method: str,
    explained: list[float] | None = None,
    label_points: bool = True,
) -> None:
    """Scatter relation projections coloured by semantic family.

    With ``label_points`` (default), each of the 36 relations is annotated with
    its name and adjustText spreads the labels; the figure is enlarged to give
    them room. Set it false (e.g. in the combined figure) for an unlabelled,
    family-coloured scatter.
    """
    fams_arr = np.asarray(families)
    ordered_fams = sorted(set(families))
    color_map = categorical_colors(ordered_fams)

    fig, ax = plt.subplots(figsize=(7.2, 6.4) if label_points else (5.6, 4.6))
    for family in ordered_fams:
        mask = fams_arr == family
        ax.scatter(
            coords[mask, 0],
            coords[mask, 1],
            s=55,
            alpha=0.9,
            linewidths=0.4,
            edgecolors="white",
            color=color_map[family],
            label=family,
        )

    if label_points:
        _annotate_points(ax, coords, relation_names)
    _label_axes(ax, method, explained)
    apply_axis_styling(ax)
    _legend_below(ax, title="Relation family", ncols=4)
    _save(fig, out_path)


def _annotate_points(ax: plt.Axes, coords: np.ndarray, names: list[str]) -> None:
    """Label scatter points, using adjustText with leader lines to avoid overlap."""
    texts = [
        ax.text(x, y, name, fontsize=5, ha="center", va="center")
        for (x, y), name in zip(coords, names)
    ]
    try:
        from adjustText import adjust_text  # noqa: PLC0415

        # Strong spreading forces + leader lines so 36 dense labels stay legible.
        adjust_text(
            texts,
            x=coords[:, 0].tolist(),
            y=coords[:, 1].tolist(),
            ax=ax,
            expand=(1.6, 2.1),
            force_text=(0.6, 1.0),
            force_static=(0.3, 0.5),
            max_move=60,
            arrowprops={"arrowstyle": "-", "lw": 0.4, "color": "0.6"},
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
    """Horizontal bar chart of per-type mean silhouette (Euclidean)."""
    items = sorted(per_type.items(), key=lambda kv: kv[1])
    names = [NODE_TYPE_NAME.get(t, t) for t, _ in items]
    values = [v for _, v in items]
    color_map = node_type_colors(list(per_type))

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
    ax.set_xlabel("Mean silhouette score", fontsize=STYLE["axis_label_fontsize"])
    apply_axis_styling(ax)
    _save(fig, out_path)


# Slightly larger fonts than the single-panel STYLE, since the combined figure
# is compact (bigger apparent text).
_COMBINED_LABEL_FS = 9
_COMBINED_TICK_FS = 8
_COMBINED_LEGEND_FS = 8


def plot_combined_figure(  # noqa: PLR0913
    umap_coords: np.ndarray,
    umap_types: list[str],
    rel_coords: np.ndarray,
    rel_names: list[str],
    rel_families: list[str],
    silhouette_per_type: dict[str, float],
    out_path: Path,
    *,
    rel_explained: list[float] | None = None,
    max_points: int = 60000,
    seed: int = 42,
) -> None:
    """Assemble the multi-panel embedding figure.

    Row 1: (a) entity UMAP and (b) per-type silhouette, both coloured by node
    type and sharing one node-type legend beneath them. Row 2: (c) the labelled
    relation PCA, coloured by semantic family with its own legend — given a full
    row so the 36 relation labels have room.
    """
    types_arr = np.asarray(umap_types)
    fams_arr = np.asarray(rel_families)
    node_colors = node_type_colors(umap_types)
    fam_colors = categorical_colors(sorted(set(rel_families)))

    if umap_coords.shape[0] > max_points:
        rng = np.random.default_rng(seed)
        sel = rng.choice(umap_coords.shape[0], size=max_points, replace=False)
        umap_coords, types_arr = umap_coords[sel], types_arr[sel]

    # Explicit axes positions (figure fractions) so the shared legend sits in a
    # tight band between the two rows without gridspec spacing surprises.
    fig = plt.figure(figsize=(8.5, 7.6))
    ax_a = fig.add_axes((0.09, 0.63, 0.37, 0.33))
    ax_b = fig.add_axes((0.61, 0.63, 0.35, 0.33))
    ax_c = fig.add_axes((0.09, 0.08, 0.87, 0.40))

    # (a) entity UMAP by node type (largest types drawn first).
    for node_type in sorted(node_colors, key=lambda t: -(types_arr == t).sum()):
        mask = types_arr == node_type
        ax_a.scatter(
            umap_coords[mask, 0],
            umap_coords[mask, 1],
            s=3,
            alpha=0.45,
            linewidths=0,
            color=node_colors[node_type],
            rasterized=True,
        )
    ax_a.set_xlabel("UMAP 1", fontsize=_COMBINED_LABEL_FS)
    ax_a.set_ylabel("UMAP 2", fontsize=_COMBINED_LABEL_FS)
    ax_a.tick_params(labelsize=_COMBINED_TICK_FS)
    apply_axis_styling(ax_a)

    # (b) per-type silhouette, node-type colours matching (a).
    items = sorted(silhouette_per_type.items(), key=lambda kv: kv[1])
    ax_b.barh(
        [NODE_TYPE_NAME.get(t, t) for t, _ in items],
        [v for _, v in items],
        color=[node_colors[t] for t, _ in items],
        edgecolor=STYLE["bar_edgecolor"],
        linewidth=STYLE["bar_linewidth"],
        alpha=STYLE["bar_alpha"],
    )
    ax_b.axvline(0, color=STYLE["bar_edgecolor"], linewidth=0.5)
    ax_b.set_xlabel("Mean silhouette score", fontsize=_COMBINED_LABEL_FS)
    ax_b.tick_params(labelsize=_COMBINED_TICK_FS)
    apply_axis_styling(ax_b)

    # Shared node-type legend in the band between rows 1 and 2 (serves a & b).
    fig.legend(
        handles=_legend_handles(
            {NODE_TYPE_NAME[t]: node_colors[t] for t in node_colors}
        ),
        title="Node type",
        loc="upper center",
        bbox_to_anchor=(0.52, 0.58),
        ncols=5,
        frameon=False,
        fontsize=_COMBINED_LEGEND_FS,
        title_fontsize=_COMBINED_LEGEND_FS,
        markerscale=1.5,
    )

    # (c) relation PCA by family, with per-relation labels.
    for family in sorted(set(rel_families)):
        mask = fams_arr == family
        ax_c.scatter(
            rel_coords[mask, 0],
            rel_coords[mask, 1],
            s=55,
            alpha=0.9,
            linewidths=0.4,
            edgecolors="white",
            color=fam_colors[family],
            label=family,
        )
    _annotate_points(ax_c, rel_coords, rel_names)
    _label_axes(ax_c, "PCA", rel_explained)
    ax_c.xaxis.label.set_fontsize(_COMBINED_LABEL_FS)
    ax_c.yaxis.label.set_fontsize(_COMBINED_LABEL_FS)
    ax_c.tick_params(labelsize=_COMBINED_TICK_FS)
    apply_axis_styling(ax_c)
    ax_c.legend(
        title="Relation family",
        loc="upper center",
        bbox_to_anchor=(0.5, -0.11),
        ncols=5,
        frameon=False,
        fontsize=_COMBINED_LEGEND_FS,
        title_fontsize=_COMBINED_LEGEND_FS,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        out_path,
        dpi=STYLE["fig_dpi"],
        bbox_inches="tight",
        facecolor=STYLE["fig_facecolor"],
    )
    plt.close(fig)
    logger.info("Wrote %s", out_path)


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
