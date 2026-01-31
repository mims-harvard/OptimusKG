import polars as pl
from kedro.pipeline import node


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
            pl.lit("cellular_component").alias("node_type"),
            pl.struct(
                [
                    pl.lit("GO").alias("source"),
                    pl.col("name"),
                    pl.col("definition"),
                    pl.col("xrefs"),  # TODO: cast to a proper list
                    pl.col("synonyms"),  # TODO: cast to a proper list
                    pl.col("ontology_description").alias("ontologyDescription"),
                    pl.col("ontology_title").alias("ontologyTitle"),
                    pl.col("ontology_license").alias("ontologyLicense"),
                    pl.col("ontology_version").alias("ontologyVersion"),
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
