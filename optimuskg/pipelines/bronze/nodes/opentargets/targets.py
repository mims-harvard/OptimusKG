import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import concat_json_partitions

log = logging.getLogger(__name__)


def process_targets(
    targets: dict[str, Callable[[], Any]], primekg_nodes_df: pl.DataFrame
) -> pd.DataFrame:
    concated_df = concat_json_partitions(targets)
    df = concated_df.select(
        pl.col("id").cast(pl.String),
        pl.col("approvedName").cast(pl.String),
        pl.col("approvedSymbol").cast(pl.String),
    )
    df = df.unique(subset=["approvedSymbol"])
    df = df.join(
        primekg_nodes_df.filter(pl.col("node_type") == "gene/protein"),
        left_on="approvedSymbol",
        right_on="node_name",
        how="inner",
    )
    return df.to_pandas()


targets_node = node(
    process_targets,
    inputs={
        "targets": "landing.opentargets.targets",
        "primekg_nodes_df": "landing.opentargets.primekg_nodes",
    },
    outputs="opentargets.targets",
    name="targets",
)
