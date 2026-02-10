"""Node metrics computation."""

import polars as pl

from .utils import (
    build_degree_map,
    collect_ontologies,
    collect_sources,
    count_non_null_properties,
    property_stats,
)


def _degree_stats_for_label(df: pl.DataFrame, degree_map: pl.DataFrame) -> dict:
    """Compute degree statistics for the nodes in *df* using *degree_map*."""
    if degree_map.height == 0:
        return {"avg": None, "std": None, "total": 0}

    degrees = (
        df.select(pl.col("id").alias("node_id"))
        .join(degree_map, on="node_id", how="left")
        .select(pl.col("degree").fill_null(0).cast(pl.Float64))
        .to_series()
    )

    stats = degrees.to_frame("d").select(
        pl.col("d").mean().alias("avg"),
        pl.col("d").std().alias("std"),
        pl.col("d").sum().cast(pl.Int64).alias("total"),
    )
    row = stats.row(0, named=True)
    return {
        "avg": row["avg"],
        "std": row["std"],
        "total": int(row["total"]) if row["total"] is not None else 0,
    }


def compute_node_metrics(
    nodes: list[pl.DataFrame],
    edges: list[pl.DataFrame],
) -> pl.DataFrame:
    """Compute per-label metrics for all node types."""
    degree_map = build_degree_map(edges)
    total_nodes = sum(df.height for df in nodes)

    rows: list[dict] = []
    for df in nodes:
        label = df["label"].unique().item()
        count = df.height
        percentage = count / total_nodes if total_nodes > 0 else 0.0

        prop_counts = count_non_null_properties(df)
        props = property_stats(prop_counts)
        deg = _degree_stats_for_label(df, degree_map)
        sources = collect_sources(df)
        ontologies = collect_ontologies(df["id"])

        rows.append(
            {
                "label": label,
                "count": count,
                "percentage": percentage,
                "properties": props,
                "degree": deg,
                "sources": sources,
                "ontologies": ontologies,
            }
        )

    schema = {
        "label": pl.String,
        "count": pl.Int64,
        "percentage": pl.Float64,
        "properties": pl.Struct(
            {"avg": pl.Float64, "std": pl.Float64, "total": pl.Int64}
        ),
        "degree": pl.Struct({"avg": pl.Float64, "std": pl.Float64, "total": pl.Int64}),
        "sources": pl.List(pl.Struct({"key": pl.String, "value": pl.Int64})),
        "ontologies": pl.List(pl.Struct({"key": pl.String, "value": pl.Int64})),
    }

    return pl.DataFrame(rows, schema=schema).sort("count", descending=True)
