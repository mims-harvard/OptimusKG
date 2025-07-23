import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    opentargets_edges: pl.DataFrame,
    phenotype_protein: pl.DataFrame,
) -> pl.DataFrame:
    opentargets_phenotype_protein = opentargets_edges.filter(
        pl.col("relation") == "phenotype_protein"
    )
    df = pl.concat([phenotype_protein, opentargets_phenotype_protein])
    df = normalize_edge_topology(df)
    return df


phenotype_protein_node = node(
    run,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "phenotype_protein": "silver.disgenet.effect_protein",
    },
    outputs="edges.phenotype_protein",
    name="phenotype_protein",
    tags=["gold"],
)
