import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    anatomy_protein: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(anatomy_protein)
    return df


anatomy_protein_edges_node = node(
    run,
    inputs={
        "anatomy_protein": "silver.bgee.anatomy_protein",
    },
    outputs="edges.anatomy_protein",
    name="anatomy_protein",
    tags=["gold"],
)
