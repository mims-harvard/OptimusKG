import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.gold.adapter.mapping import NodeMappingConfig

PHENOTYPE_NODE_MAPPING_CONFIG = NodeMappingConfig(
    id_field="id",
    label_field="type",
    properties_fields=["name", "source"],
)


def process_phenotype_nodes(
    opentargets_edges: pl.DataFrame,
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
            ],
            how="vertical",
        )
        .filter(pl.col("type") == "phenotype")
        .unique()
    )


phenotype_node = node(
    process_phenotype_nodes,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
    },
    outputs="nodes.phenotype",
    name="phenotype",
    tags=["gold"],
)
