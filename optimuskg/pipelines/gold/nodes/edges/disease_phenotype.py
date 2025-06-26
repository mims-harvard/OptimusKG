import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_disease_phenotype_edges(
    disease_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(disease_phenotype)
    return df


disease_phenotype_edges_node = node(
    process_disease_phenotype_edges,
    inputs={
        "disease_phenotype": "silver.ontology.disease_phenotype",
    },
    outputs="edges.disease_phenotype",
    name="disease_phenotype",
    tags=["gold"],
)
