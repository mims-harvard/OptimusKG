import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    exposure_molecular_function: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(exposure_molecular_function)
    return df


exposure_molecular_function_node = node(
    run,
    inputs={
        "exposure_molecular_function": "silver.exposure_molecular_function",
    },
    outputs="edges.exposure_molecular_function",
    name="exposure_molecular_function",
    tags=["gold"],
)
