import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_exposure_molecular_function_edges(
    exposure_molecular_function: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(exposure_molecular_function)
    return df


exposure_molecular_function_edges_node = node(
    process_exposure_molecular_function_edges,
    inputs={
        "exposure_molecular_function": "silver.ctd.ctd_exposure_molecular_function_interactions",
    },
    outputs="edges.exposure_molecular_function",
    name="exposure_molecular_function",
    tags=["gold"],
)
