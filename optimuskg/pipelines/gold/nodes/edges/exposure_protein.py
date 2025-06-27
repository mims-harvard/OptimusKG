import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    exposure_protein: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(exposure_protein)
    return df


exposure_protein_edges_node = node(
    run,
    inputs={
        "exposure_protein": "silver.ctd.ctd_exposure_protein_interactions",
    },
    outputs="edges.exposure_protein",
    name="exposure_protein",
    tags=["gold"],
)
