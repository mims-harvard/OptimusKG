import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    pathway_pathway: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(pathway_pathway)
    return df


pathway_pathway_edges_node = node(
    run,
    inputs={
        "pathway_pathway": "silver.reactome.pathway_pathway_interactions",
    },
    outputs="edges.pathway_pathway",
    name="pathway_pathway",
    tags=["gold"],
)
