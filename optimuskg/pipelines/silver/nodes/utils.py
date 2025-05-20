import polars as pl


def clean_edges(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.select(
            [
                "relation",
                "display_relation",
                "x_id",
                "x_type",
                "x_name",
                "x_source",
                "y_id",
                "y_type",
                "y_name",
                "y_source",
            ]
        )
        .drop_nulls()
        .unique()
        .filter(
            ~(
                (pl.col("x_id") == pl.col("y_id"))
                & (pl.col("x_type") == pl.col("y_type"))
                & (pl.col("x_source") == pl.col("y_source"))
                & (pl.col("x_name") == pl.col("y_name"))
            )
        )
    )
