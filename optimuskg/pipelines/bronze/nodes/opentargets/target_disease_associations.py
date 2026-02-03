import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    target_disease_associations: pl.DataFrame,
) -> pl.DataFrame:
    return (
        target_disease_associations.select(
            pl.col("diseaseId").alias("disease_id"),
            pl.col("targetId").alias("target_id"),
            pl.struct(
                pl.col("score"),
                pl.col("evidenceCount").alias("evidence_count"),
            ).alias("metadata"),
        )
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
