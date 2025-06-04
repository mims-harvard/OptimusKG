import polars as pl
from kedro.pipeline import node


def process_anatomical_entity_nodes(
    gene_expressions_in_anatomy: pl.DataFrame,
) -> pl.DataFrame:
    return (
        gene_expressions_in_anatomy.filter(pl.col("y_type") == "anatomy").select(
            pl.col("y_id").alias("id"),
            pl.col("y_type").alias("type"),
            pl.col("y_name").alias("name"),
            pl.col("y_source").alias("source"),
        )
    ).unique()


anatomical_entity_node = node(
    process_anatomical_entity_nodes,
    inputs={
        "gene_expressions_in_anatomy": "silver.bgee.gene_expressions_in_anatomy",
    },
    outputs="nodes.anatomical_entity",
    name="anatomical_entity",
    tags=["gold"],
)
