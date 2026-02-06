import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger("cli")


def get_ontology_prefix(node_id: str) -> str:
    """Extract the ontology prefix from a node identifier."""
    if ":" in node_id:
        return node_id.split(":", 1)[0]
    if "_" in node_id:
        return node_id.split("_", 1)[0]
    if "CHEMBL" in node_id:
        return "CHEMBL"
    if "ENSG" in node_id:
        return "Ensembl"
    return ""


def load_parquet_dir(directory: Path) -> list[pl.DataFrame]:
    """Load all non-empty parquet files from *directory*."""
    frames: list[pl.DataFrame] = []
    for path in sorted(directory.glob("*.parquet")):
        df = pl.read_parquet(path)
        if df.height > 0:
            frames.append(df)
    logger.info("Loaded %d non-empty parquet files from %s", len(frames), directory)
    return frames


def count_non_null_properties(df: pl.DataFrame) -> pl.Series:
    """Count non-null property fields per row."""
    property_fields = [field.name for field in df["properties"].dtype.fields]
    df_unnested = df.unnest("properties")
    return df_unnested.select(
        pl.sum_horizontal(
            pl.col(col).is_not_null().cast(pl.Int32) for col in property_fields
        )
    ).to_series()


def property_stats(property_counts: pl.Series) -> dict:
    """Compute summary statistics from per-row property counts."""
    stats = property_counts.to_frame("n").select(
        pl.col("n").mean().alias("avg"),
        pl.col("n").std().alias("std"),
        pl.col("n").sum().alias("total"),
    )
    row = stats.row(0, named=True)
    return {
        "avg": row["avg"],
        "std": row["std"],
        "total": int(row["total"]) if row["total"] is not None else 0,
    }


def collect_sources(
    df: pl.DataFrame, *, prefix_only: bool = False
) -> list[dict[str, object]]:
    """Extract source distribution from a DataFrame with a ``properties`` struct."""
    property_fields = [field.name for field in df["properties"].dtype.fields]
    df_unnested = df.unnest("properties")

    parts: list[pl.Series] = []

    if "sources" in property_fields:
        sources_series = (
            df_unnested.select("sources")
            .filter(pl.col("sources").is_not_null())
            .explode("sources")
            .filter(pl.col("sources").is_not_null())
            .to_series()
        )
        if prefix_only:
            sources_series = sources_series.str.split(":").list.first()
        parts.append(sources_series)

    if "source" in property_fields:
        source_series = (
            df_unnested.select("source")
            .filter(pl.col("source").is_not_null())
            .to_series()
        )
        parts.append(source_series)

    if not parts:
        return []

    combined = pl.concat(parts)
    counts = combined.value_counts().sort("count", descending=True)
    col_name = counts.columns[0]  # the original series name
    return [
        {"key": row[col_name], "value": row["count"]}
        for row in counts.iter_rows(named=True)
    ]


def collect_ontologies(id_series: pl.Series) -> list[dict[str, object]]:
    """Compute ontology-prefix distribution from a Series of node IDs."""
    prefixes = id_series.map_elements(get_ontology_prefix, return_dtype=pl.String)
    counts = prefixes.value_counts().sort("count", descending=True)
    col_name = counts.columns[0]
    return [
        {"key": row[col_name], "value": row["count"]}
        for row in counts.iter_rows(named=True)
    ]


def build_degree_map(edges: list[pl.DataFrame]) -> pl.DataFrame:
    """Build a ``node_id -> degree`` lookup from all edge DataFrames."""
    id_parts: list[pl.Series] = []
    for df in edges:
        id_parts.append(df["from"].alias("node_id"))
        id_parts.append(df["to"].alias("node_id"))

    if not id_parts:
        return pl.DataFrame(
            {
                "node_id": pl.Series([], dtype=pl.String),
                "degree": pl.Series([], dtype=pl.UInt32),
            }
        )

    all_ids = pl.concat(id_parts)
    return (
        all_ids.to_frame("node_id")
        .group_by("node_id")
        .agg(pl.len().cast(pl.UInt32).alias("degree"))
    )
