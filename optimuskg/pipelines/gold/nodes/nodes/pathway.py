import polars as pl
from kedro.pipeline import node


def run(  # noqa: PLR0913
    pathway_pathway: pl.DataFrame,
    pathway_protein: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                pathway_pathway.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                pathway_pathway.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                pathway_protein.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
            ],
            how="vertical",
        )
        .filter(pl.col("type") == "pathway")
        .unique()
    )


pathway_node = node(
    run,
    inputs={
        "pathway_pathway": "silver.pathway_pathway",
        "pathway_protein": "silver.pathway_protein",
    },
    outputs="nodes.pathway",
    name="pathway",
    tags=["gold"],
)
