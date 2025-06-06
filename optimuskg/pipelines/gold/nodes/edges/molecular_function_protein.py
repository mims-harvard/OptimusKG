import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_molecular_function_protein_edges(
    molecular_function_protein: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(molecular_function_protein)
    return df


molecular_function_protein_edges_node = node(
    process_molecular_function_protein_edges,
    inputs={
        "molecular_function_protein": "silver.ncbigene.protein_molecular_function_interactions",
    },
    outputs="edges.molecular_function_protein",
    name="molecular_function_protein",
    tags=["gold"],
)
