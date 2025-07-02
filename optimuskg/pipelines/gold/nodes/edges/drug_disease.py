import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    opentargets_edges: pl.DataFrame,
    drug_disease: pl.DataFrame,
) -> pl.DataFrame:
    df = opentargets_edges.filter(pl.col("relation") == "drug_disease")
    df = pl.concat([drug_disease, df])
    df = normalize_edge_topology(df)

    return df


drug_disease_edges_node = node(
    run,
    inputs={
        "drug_disease": "silver.drugcentral.drug_disease",
        "opentargets_edges": "silver.opentargets.opentargets_edges",
    },
    outputs="edges.drug_disease",
    name="drug_disease",
    tags=["gold"],
)
