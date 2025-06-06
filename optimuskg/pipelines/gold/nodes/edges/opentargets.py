import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def process_opentargets_edges(
    opentargets_edges: pl.DataFrame,
) -> tuple[pl.DataFrame, ...]:
    df = normalize_edge_topology(opentargets_edges)

    disease_protein = df.filter(pl.col("relation") == "disease_protein")
    strong_clinical_evidence = df.filter(
        pl.col("relation") == "strong_clinical_evidence"
    )
    phenotype_protein = df.filter(pl.col("relation") == "phenotype_protein")
    weak_clinical_evidence = df.filter(pl.col("relation") == "weak_clinical_evidence")
    disease_protein_positive = df.filter(
        pl.col("relation") == "disease_protein_positive"
    )
    indication = df.filter(pl.col("relation") == "indication")
    disease_protein_negative = df.filter(
        pl.col("relation") == "disease_protein_negative"
    )

    return (
        disease_protein,
        strong_clinical_evidence,
        phenotype_protein,
        weak_clinical_evidence,
        disease_protein_positive,
        indication,
        disease_protein_negative,
    )


opentargets_edges_node = node(
    process_opentargets_edges,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
    },
    outputs=[
        "edges.disease_protein",
        "edges.strong_clinical_evidence",
        "edges.phenotype_protein",
        "edges.weak_clinical_evidence",
        "edges.disease_protein_positive",
        "edges.indication",
        "edges.disease_protein_negative",
    ],
    name="opentargets",
    tags=["gold"],
)
