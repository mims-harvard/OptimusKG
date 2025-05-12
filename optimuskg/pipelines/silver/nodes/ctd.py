import polars as pl
from kedro.pipeline import node


def process_ctd(
    ctd_exposure_events: pl.DataFrame,
) -> pl.DataFrame:
    return ctd_exposure_events


ctd_node = node(
    process_ctd,
    inputs={"ctd_exposure_events": "bronze.ctd.ctd_exposure_events"},
    outputs="ctd.ctd_exposure_events",
    name="ctd",
    tags=["silver"],
)
