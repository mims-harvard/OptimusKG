import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_off_label_use_edges(
    drug_disease: pl.DataFrame,
) -> pl.DataFrame:
    return normalize_edge_topology(
        drug_disease.filter(pl.col("relation") == "off_label_use")
    )


off_label_use_edges_node = node(
    process_off_label_use_edges,
    inputs={
        "drug_disease": "silver.drugcentral.drug_disease",
    },
    outputs="edges.off_label_use",
    name="off_label_use",
    tags=["gold"],
)
