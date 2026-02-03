import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    drug_mechanism_of_action: pl.DataFrame,
) -> pl.DataFrame:
    return (
        drug_mechanism_of_action.select(
            pl.col("targets"),
            pl.col("chemblIds").alias("chembl_ids"),
            pl.col("mechanismOfAction").alias("mechanism_of_action"),
            pl.struct(
                pl.col("actionType").alias("action_type"),
                pl.col("targetName").alias("target_name"),
                pl.col("targetType").alias("target_type"),
                pl.col("references"),
            ).alias("metadata"),
        )
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
