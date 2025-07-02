import polars as pl
from kedro.pipeline import node


def run(
    anatomy_protein: pl.DataFrame,
    anatomy_anatomy: pl.DataFrame,
) -> pl.DataFrame:
    return pl.concat(
        [
            anatomy_protein.filter(pl.col("y_type") == "anatomy").select(
                pl.col("y_id").alias("id"),
                pl.col("y_type").alias("type"),
                pl.col("y_name").alias("name"),
                pl.col("y_source").alias("source"),
            ),
            anatomy_anatomy.select(
                pl.col("x_id").alias("id"),
                pl.col("x_type").alias("type"),
                pl.col("x_name").alias("name"),
                pl.col("x_source").alias("source"),
            ),
            anatomy_anatomy.select(
                pl.col("y_id").alias("id"),
                pl.col("y_type").alias("type"),
                pl.col("y_name").alias("name"),
                pl.col("y_source").alias("source"),
            ),
        ]
    ).unique()


anatomy_node = node(
    run,
    inputs={
        "anatomy_protein": "silver.bgee.anatomy_protein",
        "anatomy_anatomy": "silver.ontology.anatomy_anatomy",
    },
    outputs="nodes.anatomy",
    name="anatomy",
    tags=["gold"],
)
