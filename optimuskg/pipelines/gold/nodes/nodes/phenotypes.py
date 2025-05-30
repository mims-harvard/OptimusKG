import polars as pl
from kedro.pipeline import node


def process_phenotypes_nodes(
    opentargets_edges: pl.DataFrame,
) -> pl.DataFrame:
    opentargets_gene_nodes_x = (
        opentargets_edges.filter(pl.col("x_type") == "phenotype")
        .select(
            pl.col("x_id").alias("id"),
            pl.col("x_type").alias("type"),
            pl.col("x_name").alias("name"),
            pl.col("x_source").alias("source"),
        )
        .unique()
    )
    opentargets_gene_nodes_y = (
        opentargets_edges.filter(pl.col("y_type") == "phenotype")
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


phenotypes_node = node(
    process_phenotypes_nodes,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
    },
    outputs="nodes.phenotype",
    name="phenotypes",
    tags=["gold"],
)
