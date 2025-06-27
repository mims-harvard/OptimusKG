import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    gene_expressions_in_anatomy: pl.DataFrame,
) -> tuple[pl.DataFrame, ...]:
    df = normalize_edge_topology(gene_expressions_in_anatomy)
    anatomy_protein_present = df.filter(pl.col("relation") == "anatomy_protein_present")
    anatomy_protein_absent = df.filter(pl.col("relation") == "anatomy_protein_absent")
    return anatomy_protein_present, anatomy_protein_absent


anatomy_protein_edges_node = node(
    run,
    inputs={
        "gene_expressions_in_anatomy": "silver.bgee.gene_expressions_in_anatomy",
    },
    outputs=[
        "edges.anatomy_protein_present",
        "edges.anatomy_protein_absent",
    ],
    name="anatomy_protein",
    tags=["gold"],
)
