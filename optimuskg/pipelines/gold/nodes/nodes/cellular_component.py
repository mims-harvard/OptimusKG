import polars as pl
from kedro.pipeline import node


def process_cellular_component_nodes(
    protein_cellular_component_interactions: pl.DataFrame,
    exposure_cellular_component: pl.DataFrame,
    cellular_component_cellular_component: pl.DataFrame,
) -> pl.DataFrame:
    return pl.concat(
        [
            protein_cellular_component_interactions.filter(
                pl.col("y_type") == "cellular_component"
            ).select(
                pl.col("y_id").alias("id"),
                pl.col("y_type").alias("type"),
                pl.col("y_name").alias("name"),
                pl.col("y_source").alias("source"),
            ),
            exposure_cellular_component.select(
                pl.col("y_id").alias("id"),
                pl.col("y_type").alias("type"),
                pl.col("y_name").alias("name"),
                pl.col("y_source").alias("source"),
            ),
            cellular_component_cellular_component.select(
                pl.col("x_id").alias("id"),
                pl.col("x_type").alias("type"),
                pl.col("x_name").alias("name"),
                pl.col("x_source").alias("source"),
            ),
            cellular_component_cellular_component.select(
                pl.col("y_id").alias("id"),
                pl.col("y_type").alias("type"),
                pl.col("y_name").alias("name"),
                pl.col("y_source").alias("source"),
            ),
        ]
    ).unique()


cellular_component_node = node(
    process_cellular_component_nodes,
    inputs={
        "protein_cellular_component_interactions": "silver.ncbigene.protein_cellular_component_interactions",
        "exposure_cellular_component": "silver.ctd.ctd_exposure_cellular_component_interactions",
        "cellular_component_cellular_component": "silver.ontology.cellular_component_cellular_component_interactions",
    },
    outputs="nodes.cellular_component",
    name="cellular_component",
    tags=["gold"],
)
