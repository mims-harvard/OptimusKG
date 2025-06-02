import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.gold.adapter.mapping import NodeMappingConfig

CELLULAR_COMPONENT_NODE_MAPPING_CONFIG = NodeMappingConfig(
    id_field="id",
    label_field="type",
    properties_fields=["name", "source"],
)


def process_cellular_component_nodes(
    protein_cellular_component_interactions: pl.DataFrame,
) -> pl.DataFrame:
    return (
        protein_cellular_component_interactions.filter(
            pl.col("y_type") == "cellular_component"
        ).select(
            pl.col("y_id").alias("id"),
            pl.col("y_type").alias("type"),
            pl.col("y_name").alias("name"),
            pl.col("y_source").alias("source"),
        )
    ).unique()


cellular_component_node = node(
    process_cellular_component_nodes,
    inputs={
        "protein_cellular_component_interactions": "silver.ncbigene.protein_cellular_component_interactions",
    },
    outputs="nodes.cellular_component",
    name="cellular_component",
    tags=["gold"],
)
