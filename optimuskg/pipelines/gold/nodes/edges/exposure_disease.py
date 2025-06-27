import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    exposure_disease: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(exposure_disease)
    return df


exposure_disease_edges_node = node(
    run,
    inputs={
        "exposure_disease": "silver.ctd.ctd_exposure_disease_interactions",
    },
    outputs="edges.exposure_disease",
    name="exposure_disease",
    tags=["gold"],
)
