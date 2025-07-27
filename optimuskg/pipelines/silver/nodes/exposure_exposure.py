import polars as pl
from kedro.pipeline import node


def run(
    ctd_exposure_events: pl.DataFrame,
) -> pl.DataFrame:
    return (
        ctd_exposure_events.filter(
            pl.col("exposure_marker_id").is_in(
                ctd_exposure_events.select("exposure_stressor_id").unique()
            )
        )
        .select(
            [
                "exposure_stressor_name",
                "exposure_stressor_id",
                "exposure_marker",
                "exposure_marker_id",
            ]
        )
        .filter(pl.col("exposure_marker_id").is_not_null())
        .unique()
        .rename(
            {
                "exposure_stressor_id": "x_id",
                "exposure_stressor_name": "x_name",
                "exposure_marker": "y_name",
                "exposure_marker_id": "y_id",
            }
        )
        .with_columns(
            [
                pl.lit("exposure").alias("x_type"),
                pl.lit("CTD").alias("x_source"),
                pl.lit("exposure").alias("y_type"),
                pl.lit("CTD").alias("y_source"),
                pl.lit("exposure_exposure").alias("relation"),
                pl.lit("parent-child").alias("relation_type"),
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


exposure_exposure_node = node(
    run,
    inputs={"ctd_exposure_events": "bronze.ctd.ctd_exposure_events"},
    outputs="exposure_exposure",
    name="exposure_exposure",
    tags=["silver"],
)
