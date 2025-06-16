import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_contraindication_edges(
    drug_disease: pl.DataFrame,
) -> pl.DataFrame:
    return normalize_edge_topology(
        drug_disease.filter(pl.col("relation") == "contraindication")
    )


contraindication_edges_node = node(
    process_contraindication_edges,
    inputs={
        "drug_disease": "silver.drugcentral.drug_disease",
    },
    outputs="edges.contraindication",
    name="contraindication",
    tags=["gold"],
)
