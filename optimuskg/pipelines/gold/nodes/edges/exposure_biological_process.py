import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    exposure_biological_process: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(exposure_biological_process)
    return df


exposure_biological_process_edges_node = node(
    run,
    inputs={
        "exposure_biological_process": "silver.ctd.ctd_exposure_biological_process_interactions",
    },
    outputs="edges.exposure_biological_process",
    name="exposure_biological_process",
    tags=["gold"],
)
