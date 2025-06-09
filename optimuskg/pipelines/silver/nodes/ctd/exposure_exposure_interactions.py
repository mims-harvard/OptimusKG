import polars as pl
from kedro.pipeline import node


def process_ctd_exposure_exposure_interactions(
    ctd_exposure_events: pl.DataFrame,
) -> pl.DataFrame:
    # Get unique exposure stressor IDs
    unique_exposures = ctd_exposure_events.select("exposure_stressor_id").unique()

    # Filter for exposure markers that are also exposures
    df_exp_exp = ctd_exposure_events.filter(
        pl.col("exposure_marker_id").is_in(
            unique_exposures.get_column("exposure_stressor_id")
        )
    )

    # Select required columns and filter for non-null exposuremarkerid
    df_exp_exp = df_exp_exp.select(
        [
            "exposure_stressor_name",
            "exposure_stressor_id",
            "exposure_marker",
            "exposure_marker_id",
        ]
    ).filter(pl.col("exposure_marker_id").is_not_null())

    # Remove duplicates
    df_exp_exp = df_exp_exp.unique()

    # Rename columns and add new columns
    df_exp_exp = df_exp_exp.rename(
        {
            "exposure_stressor_id": "x_id",
            "exposure_stressor_name": "x_name",
            "exposure_marker": "y_name",
            "exposure_marker_id": "y_id",
        }
    )

    # Add relationship information
    df_exp_exp = df_exp_exp.with_columns(
        [
            pl.lit("exposure").alias("x_type"),
            pl.lit("CTD").alias("x_source"),
            pl.lit("exposure").alias("y_type"),
            pl.lit("CTD").alias("y_source"),
            pl.lit("exposure_exposure").alias("relation"),
            pl.lit("parent-child").alias("relation_type"),
        ]
    )

    df_exp_exp = df_exp_exp.select(
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

    return df_exp_exp


ctd_exposure_exposure_interactions_node = node(
    process_ctd_exposure_exposure_interactions,
    inputs={"ctd_exposure_events": "bronze.ctd.ctd_exposure_events"},
    outputs="ctd.ctd_exposure_exposure_interactions",
    name="ctd_exposure_exposure_interactions",
    tags=["silver"],
)
