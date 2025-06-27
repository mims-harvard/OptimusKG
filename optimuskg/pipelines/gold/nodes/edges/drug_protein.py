import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    opentargets_edges: pl.DataFrame,
    drug_protein: pl.DataFrame,
) -> pl.DataFrame:
    opentargets_drug_protein = opentargets_edges.filter(
        pl.col("relation") == "drug_protein"
    )
    df = pl.concat([drug_protein, opentargets_drug_protein])
    df = normalize_edge_topology(df)
    return df


drug_protein_edges_node = node(
    run,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "drug_protein": "silver.drugbank.drug_protein",
    },
    outputs="edges.drug_protein",
    name="drug_protein",
    tags=["gold"],
)
