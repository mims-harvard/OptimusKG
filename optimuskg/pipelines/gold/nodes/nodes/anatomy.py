import polars as pl
from kedro.pipeline import node


def run(
    gene_expressions_in_anatomy: pl.DataFrame,
    anatomy_anatomy: pl.DataFrame,
) -> pl.DataFrame:
    return pl.concat(
        [
            gene_expressions_in_anatomy.filter(pl.col("y_type") == "anatomy").select(
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
        "gene_expressions_in_anatomy": "silver.bgee.gene_expressions_in_anatomy",
        "anatomy_anatomy": "silver.ontology.anatomy_anatomy",
    },
    outputs="nodes.anatomy",
    name="anatomy",
    tags=["gold"],
)
