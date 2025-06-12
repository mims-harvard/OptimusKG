import polars as pl
from kedro.pipeline import node


def process_ctd_exposure_disease_interactions(
    ctd_exposure_events: pl.DataFrame,
    mondo_xrefs: pl.DataFrame,
    mondo_terms: pl.DataFrame,
) -> pl.DataFrame:
    # Extract relevant exposure-disease relationships
    df_exp_disease = ctd_exposure_events.select(
        ["exposure_stressor_name", "exposure_stressor_id", "disease_name", "disease_id"]
    ).filter(pl.col("disease_id").is_not_null())

    # Filter for MESH references and join with MONDO data
    mesh_xrefs = mondo_xrefs.filter(pl.col("xref_id").str.contains("MESH"))
    df_exp_disease = df_exp_disease.join(
        mesh_xrefs.select(["xref_id", "id"]),
        left_on="disease_id",
        right_on="xref_id",
        how="left",
    ).join(mondo_terms.select(["id", "name"]), left_on="id", right_on="id", how="left")

    df_exp_disease = (
        df_exp_disease.rename(
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

    return df_exp_disease


ctd_exposure_disease_interactions_node = node(
    process_ctd_exposure_disease_interactions,
    inputs={
        "ctd_exposure_events": "bronze.ctd.ctd_exposure_events",
        "mondo_xrefs": "bronze.ontology.mondo_xrefs",
        "mondo_terms": "bronze.ontology.mondo_terms",
    },
    outputs="ctd.ctd_exposure_disease_interactions",
    name="ctd_exposure_disease_interactions",
    tags=["silver"],
)
