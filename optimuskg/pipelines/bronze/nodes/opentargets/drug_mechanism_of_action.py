import logging

import polars as pl
from kedro.pipeline import node

from optimuskg.utils import to_snake_case

logger = logging.getLogger(__name__)


def run(
    drug_mechanism_of_action: pl.DataFrame,
) -> pl.DataFrame:
    key_cols = ["targets", "chemblIds", "mechanismOfAction"]
    return (
        drug_mechanism_of_action.with_columns(
            pl.struct(
                [c for c in drug_mechanism_of_action.columns if c not in key_cols]
            ).alias("metadata")
        )
        .select([*key_cols, "metadata"])
        .rename({col: to_snake_case(col) for col in key_cols})
        .unique(subset=["targets", "chembl_ids", "mechanism_of_action"])
        .sort(by=["targets", "chembl_ids", "mechanism_of_action"])
    )


drug_mechanism_of_action_node = node(
    run,
    inputs={
        "drug_mechanism_of_action": "landing.opentargets.drug_mechanism_of_action",
    },
    outputs="opentargets.drug_mechanism_of_action",
    name="drug_mechanism_of_action",
    tags=["bronze"],
)
