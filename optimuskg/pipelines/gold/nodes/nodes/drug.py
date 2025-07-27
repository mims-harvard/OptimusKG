import polars as pl
from kedro.pipeline import node


def run(
    opentargets_edges: pl.DataFrame,
    drug_drug: pl.DataFrame,
    drug_protein: pl.DataFrame,
    drug_disease: pl.DataFrame,
    drug_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
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
                drug_protein.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                drug_disease.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                drug_phenotype.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
            ],
            how="vertical",
        )
        .filter(pl.col("type") == "drug")
        .unique()
    )


drug_node = node(
    run,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "drug_drug": "silver.drug_drug",
        "drug_protein": "silver.drug_protein",
        "drug_disease": "silver.drug_disease",
        "drug_phenotype": "silver.drug_phenotype",
    },
    outputs="nodes.drug",
    name="drug",
    tags=["gold"],
)
