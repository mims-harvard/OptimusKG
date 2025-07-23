import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    drug_drug: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(drug_drug)
    return df


drug_drug_node = node(
    run,
    inputs={
        "drug_drug": "silver.drugbank.drug_drug",
    },
    outputs="edges.drug_drug",
    name="drug_drug",
    tags=["gold"],
)
