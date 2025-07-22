import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    drug_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(drug_phenotype)
    return df


drug_phenotype_node = node(
    run,
    inputs={
        "drug_phenotype": "silver.onsides.drug_phenotype",
    },
    outputs="edges.drug_phenotype",
    name="drug_phenotype",
    tags=["gold"],
)
