import logging

import polars as pl

logger = logging.getLogger(__name__)


def normalize_edge_topology(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize edge topology by removing nulls, duplicates, and self-loops.

    Args:
        df: DataFrame containing edge data with source (x_*) and target (y_*) columns

    Returns:
        DataFrame with normalized edges
    """
    required_columns = [
        "relation",
        "relation_type",
        "x_id",
        "x_type",
        "x_name",
        "x_source",
        "y_id",
        "y_type",
        "y_name",
        "y_source",
    ]

    # Validate input columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df = (
        df.select(required_columns)
        .drop_nulls()
        .unique()
        .filter(~(pl.col("x_id") == pl.col("y_id")))  # Remove self-loops
    )

    return _set_undirected_edges(df)


def _set_undirected_edges(df: pl.DataFrame) -> pl.DataFrame:
    """Identify undirected edges in a graph by finding duplicate edges with reversed source/target.

    Creates a unique key for each edge pair by sorting and concatenating source/target IDs with relation type.
    Edges that appear multiple times with reversed source/target are marked as undirected.

    Args:
        df: DataFrame containing edge data with x_id (source) and y_id (target) columns

    Returns:
        DataFrame with additional 'undirected' boolean column indicating if edge is undirected.
    """
    # Create unique key for each edge pair by sorting and joining source/target IDs with relation type
    df_with_pair_key = df.with_columns(
        pl.concat_str(
            [
                pl.struct(["x_id", "y_id"]).map_elements(
                    lambda row: "|".join(sorted([row["x_id"], row["y_id"]])),
                    return_dtype=pl.Utf8,
                ),
                pl.col("relation_type"),
            ],
            separator="|",
        ).alias("pair_key")
    )

    # Find edges that appear multiple times (with reversed source/target)
    # NOTE: Using string type instead of boolean to avoid biocypher bug on exported boolean values
    duplicated = (
        df_with_pair_key.filter(pl.col("pair_key").is_duplicated())
        .unique(subset="pair_key", keep="first")
        .with_columns(pl.lit("true").alias("undirected"))
    )

    logger.info(f"Added undirected property to {len(duplicated)} edges")

    # Keep edges that only appear once as directed
    not_duplicated = df_with_pair_key.filter(
        ~pl.col("pair_key").is_duplicated()
    ).with_columns(pl.lit("false").alias("undirected"))

    # Combine directed and undirected edges and remove temporary key
    return pl.concat([duplicated, not_duplicated]).drop("pair_key")
