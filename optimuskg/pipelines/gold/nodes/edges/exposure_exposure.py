import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    exposure_exposure: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(exposure_exposure)
    return df


exposure_exposure_node = node(
    run,
    inputs={
        "exposure_exposure": "silver.exposure_exposure",
    },
    outputs="edges.exposure_exposure",
    name="exposure_exposure",
    tags=["gold"],
)
