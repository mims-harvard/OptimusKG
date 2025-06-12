import polars as pl
from kedro.pipeline import node


def process_biological_process_nodes(
    protein_biological_process_interactions: pl.DataFrame,
    exposure_biological_process: pl.DataFrame,
) -> pl.DataFrame:
    return pl.concat(
        [
            protein_biological_process_interactions.filter(
                pl.col("y_type") == "biological_process"
            ).select(
                pl.col("y_id").alias("id"),
                pl.col("y_type").alias("type"),
                pl.col("y_name").alias("name"),
                pl.col("y_source").alias("source"),
            ),
            exposure_biological_process.select(
                pl.col("y_id").alias("id"),
                pl.col("y_type").alias("type"),
                pl.col("y_name").alias("name"),
                pl.col("y_source").alias("source"),
            ),
        ]
    ).unique()


biological_process_node = node(
    process_biological_process_nodes,
    inputs={
        "protein_biological_process_interactions": "silver.ncbigene.protein_biological_process_interactions",
        "exposure_biological_process": "silver.ctd.ctd_exposure_biological_process_interactions",
    },
    outputs="nodes.biological_process",
    name="biological_process",
    tags=["gold"],
)
