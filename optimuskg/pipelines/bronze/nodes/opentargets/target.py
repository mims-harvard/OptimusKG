import logging

import polars as pl
from kedro.pipeline import node

from optimuskg.utils import to_snake_case

logger = logging.getLogger(__name__)


def run(
    target: pl.DataFrame,
) -> pl.DataFrame:
    key_cols = ["id", "approvedSymbol", "approvedName"]
    return (
        target.with_columns(
            pl.struct([c for c in target.columns if c not in key_cols]).alias(
                "metadata"
            )
        )
        .select([*key_cols, "metadata"])
        .rename({col: to_snake_case(col) for col in key_cols})
        .unique(subset=["id", "approved_symbol"])
        .sort(by=["id", "approved_symbol"])
    )


target_node = node(
    run,
    inputs={
        "target": "landing.opentargets.target",
    },
    outputs="opentargets.target",
    name="target",
    tags=["bronze"],
)
