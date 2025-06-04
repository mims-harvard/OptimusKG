import polars as pl
from kedro.pipeline import node


def process_drug_nodes(
    opentargets_edges: pl.DataFrame,
    drug_drug: pl.DataFrame,
    drug_protein: pl.DataFrame,
) -> pl.DataFrame:
    ot_nodes = pl.concat(
        [
            opentargets_edges.select(
                pl.col("x_id").alias("id"),
                pl.col("x_type").alias("type"),
                pl.col("x_name").alias("name"),
                pl.col("x_source").alias("source"),
            ),
            opentargets_edges.select(
                pl.col("y_id").alias("id"),
                pl.col("y_type").alias("type"),
                pl.col("y_name").alias("name"),
                pl.col("y_source").alias("source"),
            ),
        ],
        how="vertical",
    )

    dd_nodes = pl.concat(
        [
            drug_drug.select(
                pl.col("x_id").alias("id"),
                pl.col("x_type").alias("type"),
                pl.col("x_name").alias("name"),
                pl.col("x_source").alias("source"),
            ),
            drug_drug.select(
                pl.col("y_id").alias("id"),
                pl.col("y_type").alias("type"),
                pl.col("y_name").alias("name"),
                pl.col("y_source").alias("source"),
            ),
        ],
        how="vertical",
    )

    dp_nodes = drug_protein.select(
        pl.col("x_id").alias("id"),
        pl.col("x_type").alias("type"),
        pl.col("x_name").alias("name"),
        pl.col("x_source").alias("source"),
    )

    all_nodes = pl.concat(
        [
            ot_nodes,
            dd_nodes,
            dp_nodes,
        ],
        how="vertical",
    )

    return all_nodes.filter(pl.col("type") == "drug").unique()


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
