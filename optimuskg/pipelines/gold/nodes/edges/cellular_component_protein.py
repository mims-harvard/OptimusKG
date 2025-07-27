import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    cellular_component_protein: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(cellular_component_protein)
    return df


cellular_component_protein_node = node(
    run,
    inputs={
        "cellular_component_protein": "silver.protein_cellular_component",
    },
    outputs="edges.cellular_component_protein",
    name="cellular_component_protein",
    tags=["gold"],
)
