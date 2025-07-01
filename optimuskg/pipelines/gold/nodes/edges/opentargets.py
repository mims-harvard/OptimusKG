import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    opentargets_edges: pl.DataFrame,
) -> tuple[pl.DataFrame, ...]:
    df = normalize_edge_topology(opentargets_edges)

    strong_clinical_evidence = df.filter(
        pl.col("relation") == "strong_clinical_evidence"
    )
    weak_clinical_evidence = df.filter(pl.col("relation") == "weak_clinical_evidence")

    return (
        strong_clinical_evidence,
        weak_clinical_evidence,
    )


opentargets_edges_node = node(
    run,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
    },
    outputs=[
        "edges.strong_clinical_evidence",
        "edges.weak_clinical_evidence",
    ],
    name="opentargets",
    tags=["gold"],
)
