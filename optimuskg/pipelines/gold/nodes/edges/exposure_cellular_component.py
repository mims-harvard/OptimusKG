import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    exposure_cellular_component: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(exposure_cellular_component)
    return df


exposure_cellular_component_node = node(
    run,
    inputs={
        "exposure_cellular_component": "silver.ctd.exposure_cellular_component",
    },
    outputs="edges.exposure_cellular_component",
    name="exposure_cellular_component",
    tags=["gold"],
)
