import polars as pl
from kedro.pipeline import node


def run(
    ctd_exposure_events: pl.DataFrame,
    mondo_xrefs: pl.DataFrame,
    mondo_terms: pl.DataFrame,
) -> pl.DataFrame:
    exposure_disease = (
        ctd_exposure_events.select(
            [
                "exposure_stressor_name",
                "exposure_stressor_id",
                "disease_name",
                "disease_id",
            ]
        )
        .filter(pl.col("disease_id").is_not_null())
        .join(
            mondo_xrefs.filter(pl.col("xref_id").str.contains("MESH")).select(
                ["xref_id", "id"]
            ),
            left_on="disease_id",
            right_on="xref_id",
            how="left",
        )
        .join(
            mondo_terms.select(["id", "name"]), left_on="id", right_on="id", how="left"
        )
        .rename(
            {
                "exposure_stressor_id": "x_id",
                "exposure_stressor_name": "x_name",
                "disease_id": "y_id",
                "disease_name": "y_name",
            }
        )
        .with_columns(
            [
                pl.lit("exposure").alias("x_type"),
                pl.lit("CTD").alias("x_source"),
                pl.lit("disease").alias("y_type"),
                pl.lit("MONDO").alias("y_source"),
                pl.lit("exposure_disease").alias("relation"),
                pl.lit("linked to").alias("relation_type"),
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

    return exposure_disease


exposure_disease_node = node(
    run,
    inputs={
        "ctd_exposure_events": "bronze.ctd.ctd_exposure_events",
        "mondo_xrefs": "bronze.ontology.mondo_xrefs",
        "mondo_terms": "bronze.ontology.mondo_terms",
    },
    outputs="ctd.exposure_disease",
    name="exposure_disease",
    tags=["silver"],
)
