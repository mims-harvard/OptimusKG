import polars as pl
from kedro.pipeline import node


def process_disease_nodes(
    opentargets_edges: pl.DataFrame,
) -> pl.DataFrame:
    opentargets_gene_nodes_x = (
        opentargets_edges.filter(pl.col("x_type") == "disease")
        .select(
            pl.col("x_id").alias("id"),
            pl.col("x_type").alias("type"),
            pl.col("x_name").alias("name"),
            pl.col("x_source").alias("source"),
        )
        .unique()
    )
    opentargets_gene_nodes_y = (
        opentargets_edges.filter(pl.col("y_type") == "disease")
        .select(
            pl.col("y_id").alias("id"),
            pl.col("y_type").alias("type"),
            pl.col("y_name").alias("name"),
            pl.col("y_source").alias("source"),
        )
        .unique()
    )

    return pl.concat(
        [
            opentargets_gene_nodes_x,
            opentargets_gene_nodes_y,
        ]
    ).unique()


disease_node = node(
    process_disease_nodes,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
    },
    outputs="nodes.disease",
    name="disease",
    tags=["gold"],
)
