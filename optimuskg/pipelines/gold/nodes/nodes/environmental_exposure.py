import polars as pl
from kedro.pipeline import node


def process_environmental_exposure_nodes(  # noqa: PLR0913
    exposure_protein: pl.DataFrame,
    exposure_exposure: pl.DataFrame,
    exposure_disease: pl.DataFrame,
    exposure_biological_process: pl.DataFrame,
    exposure_molecular_function: pl.DataFrame,
    exposure_cellular_component: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                exposure_exposure.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                exposure_exposure.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                exposure_disease.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                exposure_protein.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                exposure_biological_process.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                exposure_molecular_function.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                exposure_cellular_component.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
            ],
            how="vertical",
        )
        .filter(pl.col("type") == "exposure")
        .unique()
    )


environmental_exposure_node = node(
    process_environmental_exposure_nodes,
    inputs={
        "exposure_protein": "silver.ctd.ctd_exposure_protein_interactions",
        "exposure_exposure": "silver.ctd.ctd_exposure_exposure_interactions",
        "exposure_disease": "silver.ctd.ctd_exposure_disease_interactions",
        "exposure_biological_process": "silver.ctd.ctd_exposure_biological_process_interactions",
        "exposure_molecular_function": "silver.ctd.ctd_exposure_molecular_function_interactions",
        "exposure_cellular_component": "silver.ctd.ctd_exposure_cellular_component_interactions",
    },
    outputs="nodes.environmental_exposure",
    name="environmental_exposure",
    tags=["gold"],
)
