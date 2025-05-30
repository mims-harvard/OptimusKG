import polars as pl
from kedro.pipeline import node


def process_drug_nodes(
    opentargets_edges: pl.DataFrame,
    drug_drug: pl.DataFrame,
    drug_protein: pl.DataFrame,
) -> pl.DataFrame:
    opentargets_gene_nodes_x = (
        opentargets_edges.filter(pl.col("x_type") == "gene")
        .select(
            pl.col("x_id").alias("id"),
            pl.col("x_type").alias("type"),
            pl.col("x_name").alias("name"),
            pl.col("x_source").alias("source"),
        )
        .unique()
    )
    opentargets_gene_nodes_y = (
        opentargets_edges.filter(pl.col("y_type") == "drug")
        .select(
            pl.col("y_id").alias("id"),
            pl.col("y_type").alias("type"),
            pl.col("y_name").alias("name"),
            pl.col("y_source").alias("source"),
        )
        .unique()
    )

    drug_drug_nodes_x = (
        drug_drug.filter(pl.col("x_type") == "drug")
        .select(
            pl.col("x_id").alias("id"),
            pl.col("x_type").alias("type"),
            pl.col("x_name").alias("name"),
            pl.col("x_source").alias("source"),
        )
        .unique()
    )
    drug_drug_nodes_y = (
        drug_drug.filter(pl.col("y_type") == "drug")
        .select(
            pl.col("y_id").alias("id"),
            pl.col("y_type").alias("type"),
            pl.col("y_name").alias("name"),
            pl.col("y_source").alias("source"),
        )
        .unique()
    )

    drug_protein = (
        drug_protein.filter(pl.col("x_type") == "drug")
        .select(
            pl.col("x_id").alias("id"),
            pl.col("x_type").alias("type"),
            pl.col("x_name").alias("name"),
            pl.col("x_source").alias("source"),
        )
        .unique()
    )

    return pl.concat(
        [
            drug_drug_nodes_x,
            drug_drug_nodes_y,
            opentargets_gene_nodes_x,
            opentargets_gene_nodes_y,
            drug_protein,
        ]
    ).unique()


drug_node = node(
    process_drug_nodes,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "drug_drug": "silver.drugbank.drug_drug",
        "drug_protein": "silver.drugbank.drug_protein",
    },
    outputs="nodes.drug",
    name="drug",
    tags=["gold"],
)
