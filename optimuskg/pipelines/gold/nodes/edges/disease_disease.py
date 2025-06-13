import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_disease_disease_edges(
    disease_disease: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(disease_disease)
    return df


disease_disease_edges_node = node(
    process_disease_disease_edges,
    inputs={
        "disease_disease": "silver.ontology.mondo_disease_disease_interactions",
    },
    outputs="edges.disease_disease",
    name="disease_disease",
    tags=["gold"],
)
