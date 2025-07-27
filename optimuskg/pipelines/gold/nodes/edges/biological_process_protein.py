import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    biological_process_protein: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(biological_process_protein)
    return df


biological_process_protein_node = node(
    run,
    inputs={
        "biological_process_protein": "silver.protein_biological_process",
    },
    outputs="edges.biological_process_protein",
    name="biological_process_protein",
    tags=["gold"],
)
