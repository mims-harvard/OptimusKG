import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    phenotype_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(phenotype_phenotype)
    return df


phenotype_phenotype_node = node(
    run,
    inputs={
        "phenotype_phenotype": "silver.ontology.phenotype_phenotype",
    },
    outputs="edges.phenotype_phenotype",
    name="phenotype_phenotype",
    tags=["gold"],
)
