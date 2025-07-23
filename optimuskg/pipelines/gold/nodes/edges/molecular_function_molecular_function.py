import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    molecular_function_molecular_function: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(molecular_function_molecular_function)
    return df


molecular_function_molecular_function_node = node(
    run,
    inputs={
        "molecular_function_molecular_function": "silver.ontology.molecular_function_molecular_function",
    },
    outputs="edges.molecular_function_molecular_function",
    name="molecular_function_molecular_function",
    tags=["gold"],
)
