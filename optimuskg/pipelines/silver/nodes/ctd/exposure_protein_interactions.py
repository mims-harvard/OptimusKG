import polars as pl
from kedro.pipeline import node


def run(
    ctd_exposure_events: pl.DataFrame,
    protein_names: pl.DataFrame,
) -> pl.DataFrame:
    df_exp_prot = ctd_exposure_events.select(
        [
            "exposure_stressor_name",
            "exposure_stressor_id",
            "exposure_marker",
            "exposure_marker_id",
        ]
    )

    df_exp_prot = df_exp_prot.filter(pl.col("exposure_marker_id").is_not_null())

    df_exp_prot = df_exp_prot.filter(
        pl.col("exposure_marker_id").str.contains(r"^MESH:\d+$")
    )

    # Replace "MESH:" with "NCBIGene:" prefix to join with protein names,
    # this can be done since exposure_marker_id is a MeSH or a NCBI Gene identifier (when the ID is only a number).
    # see: https://ctdbase.org/downloads/#exposureevents
    df_exp_prot = df_exp_prot.with_columns(
        pl.col("exposure_marker_id").str.replace("MESH:", "NCBIGene:")
    )

    df_exp_prot = df_exp_prot.join(
        protein_names, left_on="exposure_marker_id", right_on="ncbi_id", how="left"
    )

    df_exp_prot = df_exp_prot.rename(
        {
            "exposure_stressor_id": "x_id",
            "exposure_stressor_name": "x_name",
            "exposure_marker_id": "y_id",
            "symbol": "y_name",
        }
    )

    df_exp_prot = df_exp_prot.with_columns(
        [
            pl.lit("exposure").alias("x_type"),
            pl.lit("CTD").alias("x_source"),
            pl.lit("gene").alias("y_type"),
            pl.lit("NCBI").alias("y_source"),
            pl.lit("exposure_protein").alias("relation"),
            pl.lit("interacts with").alias("relation_type"),
        ]
    ).drop(["exposure_marker"])

    df_exp_prot = df_exp_prot.select(
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

    return df_exp_prot


ctd_exposure_protein_interactions_node = node(
    run,
    inputs={
        "ctd_exposure_events": "bronze.ctd.ctd_exposure_events",
        "protein_names": "bronze.gene_names.protein_names",
    },
    outputs="ctd.ctd_exposure_protein_interactions",
    name="ctd_exposure_protein_interactions",
    tags=["silver"],
)
