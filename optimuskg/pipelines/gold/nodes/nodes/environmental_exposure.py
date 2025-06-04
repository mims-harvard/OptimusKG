import polars as pl
from kedro.pipeline import node


def process_environmental_exposure_nodes(  # noqa: PLR0913
    ctd_exposure_protein_interactions: pl.DataFrame,
    ctd_exposure_exposure_interactions: pl.DataFrame,
) -> pl.DataFrame:
    ee_nodes = pl.concat(
        [
            ctd_exposure_exposure_interactions.select(
                pl.col("x_id").alias("id"),
                pl.col("x_type").alias("type"),
                pl.col("x_name").alias("name"),
                pl.col("x_source").alias("source"),
            ),
            ctd_exposure_exposure_interactions.select(
                pl.col("y_id").alias("id"),
                pl.col("y_type").alias("type"),
                pl.col("y_name").alias("name"),
                pl.col("y_source").alias("source"),
            ),
        ],
        how="vertical",
    )

    ep_nodes = ctd_exposure_protein_interactions.select(
        pl.col("x_id").alias("id"),
        pl.col("x_type").alias("type"),
        pl.col("x_name").alias("name"),
        pl.col("x_source").alias("source"),
    )

    all_nodes = pl.concat(
        [
            ee_nodes,
            ep_nodes,
        ],
        how="vertical",
    )

    return all_nodes.filter(pl.col("type") == "exposure").unique()


environmental_exposure_node = node(
    process_environmental_exposure_nodes,
    inputs={
        "ctd_exposure_protein_interactions": "silver.ctd.ctd_exposure_protein_interactions",
        "ctd_exposure_exposure_interactions": "silver.ctd.ctd_exposure_exposure_interactions",
    },
    outputs="nodes.environmental_exposure",
    name="environmental_exposure",
    tags=["gold"],
)
