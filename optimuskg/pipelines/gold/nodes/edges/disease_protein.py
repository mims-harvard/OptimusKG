import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_disease_protein_edges(
    disease_protein: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(disease_protein)
    return df


disease_protein_edges_node = node(
    process_disease_protein_edges,
    inputs={
        "disease_protein": "silver.disgenet.disease_protein",
    },
    outputs="edges.disgenet_disease_protein",
    name="disease_protein",
    tags=["gold"],
)
