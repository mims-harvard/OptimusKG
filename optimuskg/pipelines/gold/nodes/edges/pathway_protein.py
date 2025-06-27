import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    pathway_protein: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(pathway_protein)
    return df


pathway_protein_edges_node = node(
    run,
    inputs={
        "pathway_protein": "silver.reactome.pathway_protein_interactions",
    },
    outputs="edges.pathway_protein",
    name="pathway_protein",
    tags=["gold"],
)
