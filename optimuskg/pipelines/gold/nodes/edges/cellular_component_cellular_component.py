import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    cellular_component_cellular_component: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(cellular_component_cellular_component)
    return df


cellular_component_cellular_component_edges_node = node(
    run,
    inputs={
        "cellular_component_cellular_component": "silver.ontology.cellular_component_cellular_component_interactions",
    },
    outputs="edges.cellular_component_cellular_component",
    name="cellular_component_cellular_component",
    tags=["gold"],
)
