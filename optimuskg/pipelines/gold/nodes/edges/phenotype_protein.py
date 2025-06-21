import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_phenotype_protein_edges(
    phenotype_protein: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(phenotype_protein)
    return df


phenotype_protein_edges_node = node(
    process_phenotype_protein_edges,
    inputs={
        "phenotype_protein": "silver.disgenet.effect_protein",
    },
    outputs="edges.disgenet_phenotype_protein",
    name="phenotype_protein",
    tags=["gold"],
)
