import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_disease_protein_edges(
    opentargets_edges: pl.DataFrame,
    disease_protein: pl.DataFrame,
) -> pl.DataFrame:
    opentargets_disease_protein = opentargets_edges.filter(pl.col("relation") == "disease_protein")
    df = pl.concat([disease_protein, opentargets_disease_protein])
    df = normalize_edge_topology(df)
    return df


disease_protein_edges_node = node(
    process_disease_protein_edges,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "disease_protein": "silver.disgenet.disease_protein",
    },
    outputs="edges.disease_protein",
    name="disease_protein",
    tags=["gold"],
)
