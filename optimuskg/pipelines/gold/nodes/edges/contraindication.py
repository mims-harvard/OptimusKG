import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    drug_disease: pl.DataFrame,
) -> pl.DataFrame:
    return normalize_edge_topology(
        drug_disease.filter(pl.col("relation") == "contraindication")
    )


contraindication_edges_node = node(
    run,
    inputs={
        "drug_disease": "silver.drugcentral.drug_disease",
    },
    outputs="edges.contraindication",
    name="contraindication",
    tags=["gold"],
)
