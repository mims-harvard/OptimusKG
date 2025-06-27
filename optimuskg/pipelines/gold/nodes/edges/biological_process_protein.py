import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    biological_process_protein: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(biological_process_protein)
    return df


biological_process_protein_edges_node = node(
    run,
    inputs={
        "biological_process_protein": "silver.ncbigene.protein_biological_process_interactions",
    },
    outputs="edges.biological_process_protein",
    name="biological_process_protein",
    tags=["gold"],
)
