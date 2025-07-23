import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    molecular_function_protein: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(molecular_function_protein)
    return df


molecular_function_protein_node = node(
    run,
    inputs={
        "molecular_function_protein": "silver.ncbigene.protein_molecular_function",
    },
    outputs="edges.molecular_function_protein",
    name="molecular_function_protein",
    tags=["gold"],
)
