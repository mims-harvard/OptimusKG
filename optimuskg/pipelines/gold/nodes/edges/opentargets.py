import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_opentargets_edges(
    opentargets_edges: pl.DataFrame,
) -> tuple[pl.DataFrame, ...]:
    df = normalize_edge_topology(opentargets_edges)

    strong_clinical_evidence = df.filter(
        pl.col("relation") == "strong_clinical_evidence"
    )
    weak_clinical_evidence = df.filter(pl.col("relation") == "weak_clinical_evidence")
    disease_protein_positive = df.filter(
        pl.col("relation") == "disease_protein_positive"
    )
    disease_protein_negative = df.filter(
        pl.col("relation") == "disease_protein_negative"
    )

    return (
        strong_clinical_evidence,
        weak_clinical_evidence,
        disease_protein_positive,
        disease_protein_negative,
    )


opentargets_edges_node = node(
    process_opentargets_edges,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
    },
    outputs=[
        "edges.strong_clinical_evidence",
        "edges.weak_clinical_evidence",
        "edges.disease_protein_positive",
        "edges.disease_protein_negative",
    ],
    name="opentargets",
    tags=["gold"],
)
