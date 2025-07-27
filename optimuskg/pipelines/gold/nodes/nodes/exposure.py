import polars as pl
from kedro.pipeline import node


def run(  # noqa: PLR0913
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


exposure_node = node(
    run,
    inputs={
        "exposure_protein": "silver.exposure_protein",
        "exposure_exposure": "silver.exposure_exposure",
        "exposure_disease": "silver.exposure_disease",
        "exposure_biological_process": "silver.exposure_biological_process",
        "exposure_molecular_function": "silver.exposure_molecular_function",
        "exposure_cellular_component": "silver.exposure_cellular_component",
    },
    outputs="nodes.exposure",
    name="exposure",
    tags=["gold"],
)
