import polars as pl
from kedro.pipeline import node


def run(
    ctd_exposure_events: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    # NOTE: Phenotypes are actually pathways
    return (
        ctd_exposure_events.select(
            [
                "exposure_stressor_name",
                "exposure_stressor_id",
                "phenotype_name",
                "phenotype_id",
            ]
        )
        .filter(pl.col("phenotype_id").is_not_null())
        .with_columns(
            pl.col("phenotype_id").str.replace("GO:", "GO_").alias("phenotype_id")
        )
        .join(go_terms, left_on="phenotype_id", right_on="id", how="inner")
        .rename(
            {
                "exposure_stressor_id": "x_id",
                "exposure_stressor_name": "x_name",
                "phenotype_id": "y_id",
                "phenotype_name": "y_name",
                "type": "y_type",
            }
        )
        .filter(pl.col("y_type") == "cellular_component")
        .with_columns(
            [
                pl.lit("exposure").alias("x_type"),
                pl.lit("CTD").alias("x_source"),
                pl.lit("cellular_component").alias("y_type"),
                pl.lit("GO").alias("y_source"),
                pl.lit("exposure_cellular_component").alias("relation"),
                pl.lit("interacts with").alias("relation_type"),
            ]
        )
        .select(
            [
                "relation",
                "relation_type",
                "x_id",
                "x_type",
                "x_name",
                "x_source",
                "y_id",
                "y_type",
                "y_name",
                "y_source",
            ]
        )
    )


exposure_cellular_component_node = node(
    run,
    inputs={
        "ctd_exposure_events": "bronze.ctd.ctd_exposure_events",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="ctd.exposure_cellular_component",
    name="exposure_cellular_component",
    tags=["silver"],
)
