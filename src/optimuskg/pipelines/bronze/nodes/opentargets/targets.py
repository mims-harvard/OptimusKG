import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import KGNodeSchema, TargetSchema, concat_json_partitions

log = logging.getLogger(__name__)


def process_targets(
    targets: dict[str, Callable[[], Any]], primekg_nodes: pl.DataFrame
) -> pd.DataFrame:
    concated_df = concat_json_partitions(targets)
    primekg_nodes_df = KGNodeSchema.convert(primekg_nodes).df
    df = TargetSchema.convert(concated_df).df
    df = df.unique(subset=["approvedSymbol"])
    df = df.join(
        primekg_nodes_df.filter(pl.col("node_type") == "gene/protein"),
        left_on="approvedSymbol",
        right_on="node_name",
        how="inner",
    )
    return df.to_pandas()  # type: ignore[no-any-return]


targets_node = node(
    process_targets,
    inputs={
        "targets": "landing.opentargets.targets",
        "primekg_nodes": "landing.opentargets.primekg_nodes",
    },
    outputs="opentargets.targets",
    name="targets",
)
