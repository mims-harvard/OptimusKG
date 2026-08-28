"""Graph-wide property metrics.

Defines the three figures quoted in the README and the docs landing page:

``property_keys``
    Number of distinct *top-level* property names across every node and edge
    table. Nested fields are not counted separately, so ``sources`` counts once
    rather than as ``sources.direct`` and ``sources.indirect``.

``property_instances``
    Number of non-null top-level property slots. This is what the JSON-encoded
    ``properties`` column materialises as key/value pairs, since null properties
    are omitted on serialisation.

``property_values``
    Number of scalar leaves reachable from those slots, recursing through
    structs and lists. A ``List(String)`` of length 4 contributes 4; a
    ``List(Struct)`` contributes one value per non-null field of each element,
    at any nesting depth.
"""

import logging
from pathlib import Path

import polars as pl

from .utils import load_parquet_dir

logger = logging.getLogger("cli")


def count_scalar_leaves(series: pl.Series) -> int:
    """Count scalar leaf values in *series*, recursing through structs and lists."""
    dtype = series.dtype

    if isinstance(dtype, pl.Struct):
        return sum(
            count_scalar_leaves(series.struct.field(field.name))
            for field in dtype.fields
        )

    if isinstance(dtype, pl.List):
        if isinstance(dtype.inner, pl.Struct | pl.List):
            return count_scalar_leaves(series.explode().drop_nulls())
        return int(series.list.len().sum() or 0)

    return int(series.is_not_null().sum())


def compute_property_metrics(frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Compute per-table property key, instance and value counts."""
    rows: list[dict[str, object]] = []

    for df in frames:
        dtype = df["properties"].dtype
        if not isinstance(dtype, pl.Struct):
            continue

        label = df["label"][0] if "label" in df.columns and df.height else ""
        for field in dtype.fields:
            series = df["properties"].struct.field(field.name)
            rows.append(
                {
                    "label": label,
                    "property_key": field.name,
                    "instances": int(series.is_not_null().sum()),
                    "values": count_scalar_leaves(series),
                }
            )

    return pl.DataFrame(
        rows,
        schema={
            "label": pl.String,
            "property_key": pl.String,
            "instances": pl.Int64,
            "values": pl.Int64,
        },
    )


def summarize_property_metrics(metrics: pl.DataFrame) -> dict[str, int]:
    """Reduce per-table property metrics to the graph-wide totals."""
    return {
        "property_keys": metrics["property_key"].n_unique(),
        "property_instances": int(metrics["instances"].sum()),
        "property_values": int(metrics["values"].sum()),
    }


def property_metrics_command(nodes_dir: Path, edges_dir: Path) -> None:
    """Report the graph-wide property key, instance and value counts."""
    frames = load_parquet_dir(nodes_dir) + load_parquet_dir(edges_dir)
    totals = summarize_property_metrics(compute_property_metrics(frames))

    logger.info(
        "%s property instances encoding %s values across %s distinct property keys",
        f"{totals['property_instances']:,}",
        f"{totals['property_values']:,}",
        f"{totals['property_keys']:,}",
    )
