import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_molecular_function_molecular_function_edges(
    molecular_function_molecular_function: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(molecular_function_molecular_function)
    return df


molecular_function_molecular_function_edges_node = node(
    process_molecular_function_molecular_function_edges,
    inputs={
        "molecular_function_molecular_function": "silver.ontology.molecular_function_molecular_function_interactions",
    },
    outputs="edges.molecular_function_molecular_function",
    name="molecular_function_molecular_function",
    tags=["gold"],
)
