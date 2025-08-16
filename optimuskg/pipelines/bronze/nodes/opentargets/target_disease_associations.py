import logging

import polars as pl
from kedro.pipeline import node

from optimuskg.utils import to_snake_case

logger = logging.getLogger(__name__)


def run(
    target_disease_associations: pl.DataFrame,
) -> pl.DataFrame:
    key_cols = ["diseaseId", "targetId"]
    return (
        target_disease_associations.with_columns(
            pl.struct(
                [c for c in target_disease_associations.columns if c not in key_cols]
            ).alias("metadata")
        )
        .select([*key_cols, "metadata"])
        .rename({col: to_snake_case(col) for col in key_cols})
        .unique(subset=["disease_id", "target_id"])
        .sort(by=["disease_id", "target_id"])
    )


target_disease_associations_node = node(
    run,
    inputs={
        "target_disease_associations": "landing.opentargets.target_disease_associations",
    },
    outputs="opentargets.target_disease_associations",
    name="target_disease_associations",
    tags=["bronze"],
)
