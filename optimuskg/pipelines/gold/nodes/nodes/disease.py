import polars as pl
from kedro.pipeline import node


def process_disease_nodes(
    opentargets_edges: pl.DataFrame,
    disease_disease_edges: pl.DataFrame,
    exposure_disease_edges: pl.DataFrame,
    drug_disease: pl.DataFrame,
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
                disease_disease_edges.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                disease_disease_edges.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                exposure_disease_edges.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                drug_disease.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
            ],
            how="vertical",
        )
        .filter(pl.col("type") == "disease")
        .unique()
    )


disease_node = node(
    process_disease_nodes,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "disease_disease_edges": "silver.ontology.mondo_disease_disease_interactions",
        "exposure_disease_edges": "silver.ctd.ctd_exposure_disease_interactions",
        "drug_disease": "silver.drugcentral.drug_disease",
    },
    outputs="nodes.disease",
    name="disease",
    tags=["gold"],
)
