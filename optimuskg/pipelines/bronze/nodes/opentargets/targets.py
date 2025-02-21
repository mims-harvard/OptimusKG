import logging
from collections.abc import Callable
from typing import Any, Final, final

import pandas as pd
import polars as pl
from kedro.pipeline import node
from typedframe import PolarsTypedFrame

from .utils import concat_json_partitions

log = logging.getLogger(__name__)


@final
class TargetSchema(PolarsTypedFrame):
    schema: Final[dict[str, type[pl.DataType] | pl.List]] = {
        "id": pl.String,
        "approvedSymbol": pl.String,
        "approvedName": pl.String,
    }


def process_targets(
    targets: dict[str, Callable[[], Any]], primekg_nodes_df: pl.DataFrame
) -> pd.DataFrame:
    concated_df = concat_json_partitions(targets)
    concated_df = concated_df.select("id", "approvedName", "approvedSymbol")
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
        "primekg_nodes_df": "landing.opentargets.primekg_nodes",
    },
    outputs="opentargets.targets",
    name="targets",
)
