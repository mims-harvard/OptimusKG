import logging

import polars as pl
from kedro.pipeline import node

from optimuskg.utils import to_snake_case

logger = logging.getLogger(__name__)


def run(
    drug_indication: pl.DataFrame,
) -> pl.DataFrame:
    key_cols = ["id", "approvedIndications"]
    return (
        drug_indication.with_columns(
            pl.struct([c for c in drug_indication.columns if c not in key_cols]).alias(
                "metadata"
            )
        )
        .select([*key_cols, "metadata"])
        .rename({col: to_snake_case(col) for col in key_cols})
        .unique(subset="id")
        .sort(by="id")
    )


drug_indication_node = node(
    run,
    inputs={
        "drug_indication": "landing.opentargets.drug_indication",
    },
    outputs="opentargets.drug_indication",
    name="drug_indication",
    tags=["bronze"],
)
