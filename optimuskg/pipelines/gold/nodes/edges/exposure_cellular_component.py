import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_exposure_cellular_component_edges(
    exposure_cellular_component: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(exposure_cellular_component)
    return df


exposure_cellular_component_edges_node = node(
    process_exposure_cellular_component_edges,
    inputs={
        "exposure_cellular_component": "silver.ctd.ctd_exposure_cellular_component_interactions",
    },
    outputs="edges.exposure_cellular_component",
    name="exposure_cellular_component",
    tags=["gold"],
)
