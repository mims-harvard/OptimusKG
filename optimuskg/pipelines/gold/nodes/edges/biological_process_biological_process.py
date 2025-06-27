import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    biological_process_biological_process: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(biological_process_biological_process)
    return df


biological_process_biological_process_edges_node = node(
    run,
    inputs={
        "biological_process_biological_process": "silver.ontology.biological_process_biological_process_interactions",
    },
    outputs="edges.biological_process_biological_process",
    name="biological_process_biological_process",
    tags=["gold"],
)
