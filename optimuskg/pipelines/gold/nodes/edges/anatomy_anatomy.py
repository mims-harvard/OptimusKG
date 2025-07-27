import polars as pl
from kedro.pipeline import node

from .utils import normalize_edge_topology


def run(
    anatomy_anatomy: pl.DataFrame,
) -> pl.DataFrame:
    df = normalize_edge_topology(anatomy_anatomy)
    return df


anatomy_anatomy_node = node(
    run,
    inputs={
        "anatomy_anatomy": "silver.anatomy_anatomy",
    },
    outputs="edges.anatomy_anatomy",
    name="anatomy_anatomy",
    tags=["gold"],
)
