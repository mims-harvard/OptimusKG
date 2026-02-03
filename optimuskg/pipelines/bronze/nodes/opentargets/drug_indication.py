import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    drug_indication: pl.DataFrame,
) -> pl.DataFrame:
    return (
        drug_indication.select(
            pl.col("id"),
            pl.col("approvedIndications").alias("approved_indications"),
            pl.struct(
                pl.col("indications")
                .list.eval(
                    pl.struct(
                        pl.element().struct.field("disease"),
                        pl.element().struct.field("efoName").alias("efo_name"),
                        pl.element()
                        .struct.field("maxPhaseForIndication")
                        .alias("max_phase_for_indication"),
                        pl.element().struct.field("references"),
                    )
                )
                .alias("indications"),
                pl.col("indicationCount").alias("indication_count"),
            ).alias("metadata"),
        )
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
