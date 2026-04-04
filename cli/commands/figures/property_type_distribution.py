"""Property type distribution figure: stacked barplot of metadata attribute types.

For each node type and each edge type in the knowledge graph, shows the
proportion of leaf-level metadata attributes (inside the ``properties``
struct) that belong to each primitive data-type category.  Struct and
List(Struct) fields are recursively flattened so that only primitive types
remain: String, Boolean, Integer, Float, List(String), List(Boolean),
List(Integer), and List(Float).

The ``sources`` property is excluded because it is universal across all
entity types and would dominate the distribution without adding information.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from matplotlib.axes import Axes

from optimuskg.pipelines.silver.nodes.constants import Node

from . import style  # noqa: F401 — applies global rcParams
from .style import (
    BLUE_SCALE,
    STYLE,
    apply_axis_styling,
    apply_legend_styling,
)

# ──────────────────────────────────────────────────────────────────────
# Canonical ordering
# ──────────────────────────────────────────────────────────────────────

# Node ordering (alphabetical by abbreviation).
_NODE_ORDER = [member.value for member in Node]

# Filename stem → node abbreviation.
_NODE_FILE_TO_LABEL: dict[str, str] = {
    "anatomy": Node.ANATOMY.value,
    "biological_process": Node.BIOLOGICAL_PROCESS.value,
    "cellular_component": Node.CELLULAR_COMPONENT.value,
    "disease": Node.DISEASE.value,
    "drug": Node.DRUG.value,
    "exposure": Node.EXPOSURE.value,
    "gene": Node.GENE.value,
    "molecular_function": Node.MOLECULAR_FUNCTION.value,
    "pathway": Node.PATHWAY.value,
    "phenotype": Node.PHENOTYPE.value,
}

# Filename stem → edge label (e.g. "disease_gene" → "DIS-GEN").
_EDGE_PART_TO_NODE: dict[str, str] = {
    "anatomy": Node.ANATOMY.value,
    "biological_process": Node.BIOLOGICAL_PROCESS.value,
    "cellular_component": Node.CELLULAR_COMPONENT.value,
    "disease": Node.DISEASE.value,
    "drug": Node.DRUG.value,
    "exposure": Node.EXPOSURE.value,
    "gene": Node.GENE.value,
    "molecular_function": Node.MOLECULAR_FUNCTION.value,
    "pathway": Node.PATHWAY.value,
    "phenotype": Node.PHENOTYPE.value,
}

# ──────────────────────────────────────────────────────────────────────
# Data-type categories
# ──────────────────────────────────────────────────────────────────────

_DTYPE_CATEGORIES = [
    "String",
    "Boolean",
    "Integer",
    "Float",
    "List(String)",
    "List(Boolean)",
    "List(Integer)",
    "List(Float)",
]

# Tailwind/shadcn-ui colour palette for the 8 dtype categories.
_CATEGORY_COLORS: dict[str, str] = {
    "String": BLUE_SCALE["400"],  # blue-400
    "Boolean": "#F59E0B",  # amber-500
    "Integer": "#10B981",  # emerald-500
    "Float": "#EAB308",  # yellow-500
    "List(String)": BLUE_SCALE["700"],  # blue-700
    "List(Boolean)": "#F97316",  # orange-500
    "List(Integer)": "#8B5CF6",  # violet-500
    "List(Float)": "#64748B",  # slate-500
}

_INTEGER_TYPES = frozenset(
    {
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
    }
)

_FLOAT_TYPES = frozenset({pl.Float32, pl.Float64})


_STRING_TYPES = frozenset({pl.String, pl.Utf8})
_BOOLEAN_TYPES = frozenset({pl.Boolean})

# Unified lookup: DataType/DataTypeClass → primitive label.
_PRIMITIVE_MAP: dict[pl.DataType | pl.DataTypeClass, str] = {
    pl.String: "String",
    pl.Utf8: "String",
    pl.Boolean: "Boolean",
    **{t: "Integer" for t in _INTEGER_TYPES},
    **{t: "Float" for t in _FLOAT_TYPES},
}


def _primitive_label(dtype: pl.DataType | pl.DataTypeClass) -> str | None:
    """Return the primitive category label for a scalar type, or ``None``."""
    return _PRIMITIVE_MAP.get(dtype) or _PRIMITIVE_MAP.get(type(dtype))


def _flatten_dtype(
    dtype: pl.DataType | pl.DataTypeClass,
    *,
    inside_list: bool = False,
) -> dict[str, int]:
    """Recursively flatten *dtype* into primitive category counts.

    Struct fields are recursed into so that every leaf is a primitive
    (String, Boolean, Integer, Float).  When a ``List`` wraps a composite
    type the ``inside_list`` flag propagates so that each leaf becomes the
    corresponding ``List(…)`` category.
    """
    # Scalar primitive.
    prim = _primitive_label(dtype)
    if prim is not None:
        key = f"List({prim})" if inside_list else prim
        return {key: 1}

    # Struct → recurse into each field.
    if isinstance(dtype, pl.Struct):
        counts: dict[str, int] = {}
        for field in dtype.fields:
            for k, v in _flatten_dtype(field.dtype, inside_list=inside_list).items():
                counts[k] = counts.get(k, 0) + v
        return counts

    # List → unwrap and recurse.
    if isinstance(dtype, pl.List):
        inner = dtype.inner
        if inner is not None:
            return _flatten_dtype(inner, inside_list=True)
        return {"List(String)": 1}  # conservative fallback

    # Unknown type — treat as String.
    key = "List(String)" if inside_list else "String"
    return {key: 1}


def _edge_label_from_stem(stem: str) -> str:
    """Derive an edge label such as ``DIS-GEN`` from a file stem like
    ``disease_gene``.

    The stem is split into two parts by matching the longest known
    node-name prefix first (greedy left-to-right).
    """
    # Try every known prefix from longest to shortest to avoid ambiguity
    # (e.g. "biological_process" before "biological").
    prefixes: list[str] = list(_EDGE_PART_TO_NODE)
    prefixes.sort(key=len, reverse=True)
    src_name: str | None = None
    remainder: str = ""
    for prefix in prefixes:
        if stem.startswith(prefix + "_"):
            src_name = prefix
            remainder = stem[len(prefix) + 1 :]
            break

    if src_name is None:
        # Last resort: take the portion before the first underscore.
        src_name, _, remainder = stem.partition("_")

    src_label = _EDGE_PART_TO_NODE.get(src_name, src_name.upper())
    dst_label = _EDGE_PART_TO_NODE.get(remainder, remainder.upper())
    return f"{src_label}-{dst_label}"


# ──────────────────────────────────────────────────────────────────────
# compute_data
# ──────────────────────────────────────────────────────────────────────


def _collect_property_types(
    directory: Path,
    entity_kind: str,
) -> list[dict[str, object]]:
    """Introspect all parquet files in *directory* and return one row per
    (entity_type, dtype_category) with the attribute count.
    """
    rows: list[dict[str, object]] = []
    for path in sorted(directory.glob("*.parquet")):
        schema = pl.read_parquet_schema(path)
        stem = path.stem

        # Derive the human-readable label.
        if entity_kind == "node":
            label = _NODE_FILE_TO_LABEL.get(stem, stem.upper())
        else:
            label = _edge_label_from_stem(stem)

        # Locate the properties struct.
        props_dtype = schema.get("properties")
        if props_dtype is None or not isinstance(props_dtype, pl.Struct):
            continue

        # Count leaf-level attribute types.
        category_counts: dict[str, int] = {c: 0 for c in _DTYPE_CATEGORIES}
        for field in props_dtype.fields:
            for cat, n in _flatten_dtype(field.dtype).items():
                category_counts[cat] = category_counts.get(cat, 0) + n

        for cat, count in category_counts.items():
            rows.append(
                {
                    "entity_type": label,
                    "entity_kind": entity_kind,
                    "dtype_category": cat,
                    "count": count,
                }
            )

    return rows


def compute_data(nodes_dir: Path, edges_dir: Path) -> pl.DataFrame:
    """Compute property-type distribution for every node and edge type.

    Returns a long-format ``pl.DataFrame`` with columns:

    * ``entity_type`` – abbreviation (e.g. ``GEN``, ``DIS-PRO``)
    * ``entity_kind`` – ``"node"`` or ``"edge"``
    * ``dtype_category`` – one of the eight primitive type categories
    * ``count`` – number of properties in that category
    """
    node_rows = _collect_property_types(nodes_dir, "node")
    edge_rows = _collect_property_types(edges_dir, "edge")

    return pl.DataFrame(
        node_rows + edge_rows,
        schema={
            "entity_type": pl.String,
            "entity_kind": pl.String,
            "dtype_category": pl.String,
            "count": pl.Int64,
        },
    )


# ──────────────────────────────────────────────────────────────────────
# render_plot
# ──────────────────────────────────────────────────────────────────────


def _draw_stacked_bars(  # noqa: PLR0913
    ax: Axes,
    entity_types: list[str],
    values_by_cat: dict[str, list[float]],
    title: str,
    *,
    rotate_labels: bool = False,
    y_limit: float | None = None,
) -> None:
    """Draw a single stacked-bar panel on *ax*."""
    x = np.arange(len(entity_types))
    bar_width = 0.875  # tighter than default (0.75) — halved gap
    bottom = np.zeros(len(entity_types))

    for cat in _DTYPE_CATEGORIES:
        values = np.array(values_by_cat[cat])
        ax.bar(
            x,
            values,
            width=bar_width,
            bottom=bottom,
            label=cat,
            color=_CATEGORY_COLORS[cat],
            edgecolor="none",
            linewidth=0,
            alpha=STYLE["bar_alpha"],
        )
        bottom += values

    # Show total count above each bar.
    for i, total in enumerate(bottom):
        if total > 0:
            label = f"{int(total)}" if total == int(total) else f"{total:.1f}"
            ax.text(
                x[i],
                total,
                label,
                ha="center",
                va="bottom",
                fontsize=STYLE["value_label_fontsize"],
            )

    ax.set_xticks(x)
    rotation = 45 if rotate_labels else 0
    ha = "right" if rotate_labels else "center"
    ax.set_xticklabels(entity_types, rotation=rotation, ha=ha, fontsize=8)
    ax.tick_params(axis="x", length=0)
    if y_limit is not None:
        ax.set_ylim(0, y_limit)
    title_pad: int = STYLE["title_pad"]  # type: ignore[assignment]
    ax.set_title(
        title,
        fontsize=STYLE["title_fontsize"],
        fontweight=STYLE["title_fontweight"],
        pad=title_pad,
    )

    apply_axis_styling(ax)


# ──────────────────────────────────────────────────────────────────────
# Helpers for pivoting data
# ──────────────────────────────────────────────────────────────────────


def _sort_by_total(df: pl.DataFrame) -> list[str]:
    """Return entity types sorted by ascending total property count."""
    totals = df.group_by("entity_type").agg(pl.col("count").sum().alias("total"))
    return totals.sort("total")["entity_type"].to_list()


def _to_percentages(
    df: pl.DataFrame,
) -> tuple[list[str], dict[str, list[float]]]:
    """Pivot *df* to per-category percentage lists."""
    totals = df.group_by("entity_type").agg(pl.col("count").sum().alias("total"))
    df = df.join(totals, on="entity_type")
    df = df.with_columns(
        (pl.col("count") / pl.col("total")).alias("pct"),
    )
    entity_types = _sort_by_total(df)
    pct_map: dict[str, list[float]] = {cat: [] for cat in _DTYPE_CATEGORIES}
    for et in entity_types:
        sub = df.filter(pl.col("entity_type") == et)
        for cat in _DTYPE_CATEGORIES:
            row = sub.filter(pl.col("dtype_category") == cat)
            pct_map[cat].append(row["pct"][0] if row.height > 0 else 0.0)
    return entity_types, pct_map


def _to_counts(
    df: pl.DataFrame,
) -> tuple[list[str], dict[str, list[float]]]:
    """Pivot *df* to per-category raw count lists."""
    entity_types = _sort_by_total(df)
    cnt_map: dict[str, list[float]] = {cat: [] for cat in _DTYPE_CATEGORIES}
    for et in entity_types:
        sub = df.filter(pl.col("entity_type") == et)
        for cat in _DTYPE_CATEGORIES:
            row = sub.filter(pl.col("dtype_category") == cat)
            cnt_map[cat].append(float(row["count"][0]) if row.height > 0 else 0.0)
    return entity_types, cnt_map


# ──────────────────────────────────────────────────────────────────────
# Figure rendering
# ──────────────────────────────────────────────────────────────────────


def _render_figure(
    data: pl.DataFrame,
    out_path: Path,
    *,
    normalize: bool,
) -> None:
    """Shared implementation for both normalised and raw-count figures."""
    nodes_df = data.filter(pl.col("entity_kind") == "node")
    edges_df = data.filter(pl.col("entity_kind") == "edge")

    pivot_fn = _to_percentages if normalize else _to_counts
    node_types, node_vals = pivot_fn(nodes_df)
    edge_types, edge_vals = pivot_fn(edges_df)

    n_nodes = len(node_types)
    n_edges = len(edge_types)

    width_ratio_nodes = max(n_nodes, 6)
    width_ratio_edges = n_edges

    fig, (ax_nodes, ax_edges) = plt.subplots(
        1,
        2,
        figsize=(12, 4),
        gridspec_kw={"width_ratios": [width_ratio_nodes, width_ratio_edges]},
        sharey=normalize,
    )

    y_limit = 1.0 if normalize else None
    _draw_stacked_bars(ax_nodes, node_types, node_vals, "Node types", y_limit=y_limit)

    # Two-line labels for edge types: "DIS-PRO" → "DIS\nPRO"
    edge_labels = [et.replace("-", "\n") for et in edge_types]
    _draw_stacked_bars(
        ax_edges,
        edge_labels,
        edge_vals,
        "Edge types",
        y_limit=y_limit,
    )

    ylabel = "Percentage" if normalize else "Number of properties"
    ax_nodes.set_ylabel(
        ylabel,
        fontsize=STYLE["axis_label_fontsize"],
        fontweight=STYLE["axis_label_fontweight"],
    )

    # Shared legend above both panels.
    handles, labels = ax_nodes.get_legend_handles_labels()
    legend = fig.legend(
        handles,
        labels,
        loc="upper center",
        ncol=len(_DTYPE_CATEGORIES),
        bbox_to_anchor=(0.5, 1.02),
        fontsize=STYLE["legend_fontsize"],
        frameon=False,
    )
    apply_legend_styling(legend)

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.savefig(
        out_path,
        dpi=STYLE["fig_dpi"],
        facecolor=STYLE["fig_facecolor"],
        bbox_inches="tight",
    )
    plt.close(fig)


def render_plot(data: pl.DataFrame, out_path: Path) -> None:
    """Render two-panel stacked barplots and save as PDF.

    Produces two files:

    * *out_path* — bars normalised to percentages (0–1).
    * *out_path* with ``_counts`` suffix — bars showing raw property counts.
    """
    # Normalised (percentage) version.
    _render_figure(data, out_path, normalize=True)

    # Raw-counts version.
    counts_path = out_path.with_stem(out_path.stem + "_counts")
    _render_figure(data, counts_path, normalize=False)
