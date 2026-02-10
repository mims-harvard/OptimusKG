import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Node


def run(
    cellular_component_protein: pl.DataFrame,
    exposure_cellular_component: pl.DataFrame,
    cellular_component_cellular_component: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                cellular_component_protein.select(pl.col("from").alias("id")),
                exposure_cellular_component.select(pl.col("to").alias("id")),
                cellular_component_cellular_component.select(
                    pl.concat_list(["from", "to"]).explode().alias("id")
                ),
            ]
        )
        .unique(subset="id")
        .join(go_terms, left_on="id", right_on="id", how="inner")
        .select(
            pl.col("id"),
            pl.lit(Node.CELLULAR_COMPONENT).alias("label"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit(["GO"]).alias("direct"),
                            pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                        ]
                    ).alias("sources"),
                    pl.col("name"),
                    pl.col("definition"),
                    pl.col("xrefs"),
                    pl.col("synonyms"),
                    pl.col("ontology"),
                ]
            ).alias("properties"),
        )
        .sort(by="id")
    )


cellular_component_node = node(
    run,
    inputs={
        "cellular_component_protein": "silver.edges.cellular_component_protein",
        "exposure_cellular_component": "silver.edges.exposure_cellular_component",
        "cellular_component_cellular_component": "silver.edges.cellular_component_cellular_component",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="nodes.cellular_component",
    name="cellular_component",
    tags=["silver"],
)
