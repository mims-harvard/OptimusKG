import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_indication_edges(
    drug_disease: pl.DataFrame,
    opentargets_edges: pl.DataFrame,
) -> pl.DataFrame:
    return normalize_edge_topology(
        pl.concat([drug_disease, opentargets_edges]).filter(
            pl.col("relation") == "indication"
        )
    )


indication_edges_node = node(
    process_indication_edges,
    inputs={
        "drug_disease": "silver.drugcentral.drug_disease",
        "opentargets_edges": "silver.opentargets.opentargets_edges",
    },
    outputs="edges.indication",
    name="indication",
    tags=["gold"],
)
