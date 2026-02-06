"""Edge metrics computation."""

import polars as pl

from .utils import (
    collect_ontologies,
    collect_sources,
    count_non_null_properties,
    property_stats,
)


def _directionality_stats(df: pl.DataFrame) -> dict:
    """Compute directed/undirected counts from the top-level ``undirected`` column."""
    if "undirected" in df.columns:
        undirected_count = df.filter(pl.col("undirected")).height
        directed_count = df.filter(~pl.col("undirected")).height
    else:
        directed_count = df.height
        undirected_count = 0
    return {"directed": directed_count, "undirected": undirected_count}


def _combine_ontologies(df: pl.DataFrame) -> list[dict[str, object]]:
    """Collect ontology-prefix distributions from both ``from`` and ``to`` IDs.

    Merges the counts from both columns into a single sorted list of
    ``{"key": str, "value": int}`` dicts.
    """
    from_counts = collect_ontologies(df["from"])
    to_counts = collect_ontologies(df["to"])

    merged: dict[str, int] = {}
    for entry in from_counts:
        merged[entry["key"]] = merged.get(entry["key"], 0) + entry["value"]
    for entry in to_counts:
        merged[entry["key"]] = merged.get(entry["key"], 0) + entry["value"]

    return [
        {"key": k, "value": v}
        for k, v in sorted(merged.items(), key=lambda item: item[1], reverse=True)
    ]


def compute_edge_metrics(edges: list[pl.DataFrame]) -> pl.DataFrame:
    """Compute per-label metrics for all edge types."""
    total_edges = sum(df.height for df in edges)

    rows: list[dict] = []
    for df in edges:
        label = df["label"].unique().item()
        count = df.height
        percentage = count / total_edges if total_edges > 0 else 0.0

        prop_counts = count_non_null_properties(df)
        props = property_stats(prop_counts)
        directionality = _directionality_stats(df)
        sources = collect_sources(df, prefix_only=True)
        ontologies = _combine_ontologies(df)

        rows.append(
            {
                "label": label,
                "count": count,
                "percentage": percentage,
                "properties": props,
                "directionality": directionality,
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
        "directionality": pl.Struct({"directed": pl.Int64, "undirected": pl.Int64}),
        "sources": pl.List(pl.Struct({"key": pl.String, "value": pl.Int64})),
        "ontologies": pl.List(pl.Struct({"key": pl.String, "value": pl.Int64})),
    }

    return pl.DataFrame(rows, schema=schema).sort("count", descending=True)
