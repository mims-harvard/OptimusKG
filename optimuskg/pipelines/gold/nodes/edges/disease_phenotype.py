import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    disease_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(disease_phenotype)
    return df


disease_phenotype_node = node(
    run,
    inputs={
        "disease_phenotype": "silver.ontology.disease_phenotype",
    },
    outputs="edges.disease_phenotype",
    name="disease_phenotype",
    tags=["gold"],
)
