import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.utils import clean_edges


def process_ctd_exposure_protein_interactions(
    ctd_exposure_events: pl.DataFrame,
    protein_names: pl.DataFrame,
) -> pl.DataFrame:
    # Get exposure events with marker ids
    df_exp_prot = ctd_exposure_events.select(
        [
            "exposure_stressor_name",
            "exposure_stressor_id",  # MESH ID (https://github.com/biolink/biolink-model/blob/56bb3a024d8c88c0ce75267cc7d0b8a1baf7f88e/project/prefixmap/biolink_model_prefix_map.json#L109)
            "exposure_marker",
            "exposure_marker_id",  # MESH or NCBI ID
        ]
    )

    # Filter for non-null exposure marker ids
    df_exp_prot = df_exp_prot.filter(pl.col("exposure_marker_id").is_not_null())

    # Filter for numeric values in exposure_marker_id
    df_exp_prot = df_exp_prot.filter(
        pl.col("exposure_marker_id").str.contains(r"^\d+$")
    )

    df_exp_prot = df_exp_prot.with_columns(
        pl.col("exposure_marker_id").map_elements(
            lambda x: f"NCBIGene:{x}",
            return_dtype=pl.Utf8,
        )  # Add "NCBIGene:" prefix to ncbi_id column to match biolink schema
    )

    # Merge with protein names
    df_exp_prot = df_exp_prot.join(
        protein_names, left_on="exposure_marker_id", right_on="ncbi_id", how="left"
    )

    # Rename columns and add new columns
    df_exp_prot = df_exp_prot.rename(
        {
            "exposure_stressor_id": "x_id",
            "exposure_stressor_name": "x_name",
            "exposure_marker_id": "y_id",
            "symbol": "y_name",
        }
    )

    # Add relationship information
    df_exp_prot = df_exp_prot.with_columns(
        [
            pl.lit("exposure").alias("x_type"),
            pl.lit("CTD").alias("x_source"),
            pl.lit("gene").alias("y_type"),
            pl.lit("NCBI").alias("y_source"),
            pl.lit("exposure_protein").alias("relation"),
            pl.lit("interacts with").alias("display_relation"),
        ]
    )

    df_exp_prot = clean_edges(df_exp_prot)

    return df_exp_prot


ctd_exposure_protein_interactions_node = node(
    process_ctd_exposure_protein_interactions,
    inputs={
        "ctd_exposure_events": "bronze.ctd.ctd_exposure_events",
        "protein_names": "bronze.gene_names.protein_names",
    },
    outputs="ctd.ctd_exposure_protein_interactions",
    name="ctd_exposure_protein_interactions",
    tags=["silver"],
)
