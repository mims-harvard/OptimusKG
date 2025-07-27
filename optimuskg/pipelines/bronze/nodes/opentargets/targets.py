from collections.abc import Callable
from typing import Any

import polars as pl
from kedro.pipeline import node

from .utils import concat_json_partitions


def run(targets: dict[str, Callable[[], Any]]) -> pl.DataFrame:
    df = (
        concat_json_partitions(targets)
        .select(
            pl.col("id").cast(pl.String),
            pl.col("approvedName").cast(pl.String).alias("name"),
            pl.col("approvedSymbol").cast(pl.String).alias("symbol"),
            pl.lit("opentargets").alias("source"),
        )
        .unique(subset=["symbol"])
    )
    df = df.sort(by=sorted(df.columns))
    return df


targets_node = node(
    run,
    inputs={
        "targets": "landing.opentargets.targets",
    },
    outputs="opentargets.targets",
    name="targets",
    tags=["bronze"],
)
