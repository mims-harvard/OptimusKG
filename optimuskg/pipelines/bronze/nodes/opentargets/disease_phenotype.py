import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    disease_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    return (
        disease_phenotype.select(
            pl.col("disease"),
            pl.col("phenotype"),
            pl.struct(
                pl.col("evidence")
                .list.eval(
                    pl.struct(
                        pl.element().struct.field("aspect"),
                        pl.element().struct.field("bioCuration").alias("bio_curation"),
                        pl.element()
                        .struct.field("diseaseFromSourceId")
                        .alias("disease_from_source_id"),
                        pl.element()
                        .struct.field("diseaseFromSource")
                        .alias("disease_from_source"),
                        pl.element().struct.field("diseaseName").alias("disease_name"),
                        pl.element()
                        .struct.field("evidenceType")
                        .alias("evidence_type"),
                        pl.element().struct.field("frequency"),
                        pl.element().struct.field("modifiers"),
                        pl.element().struct.field("onset"),
                        pl.element().struct.field("qualifier"),
                        pl.element()
                        .struct.field("qualifierNot")
                        .alias("qualifier_not"),
                        pl.element().struct.field("references"),
                        pl.element().struct.field("sex"),
                        pl.element().struct.field("resource"),
                    )
                )
                .alias("evidence"),
            ).alias("metadata"),
        )
        .unique(subset=["disease", "phenotype"])
        .sort(by=["disease", "phenotype"])
    )


disease_phenotype_node = node(
    run,
    inputs={
        "disease_phenotype": "landing.opentargets.disease_phenotype",
    },
    outputs="opentargets.disease_phenotype",
    name="disease_phenotype",
    tags=["bronze"],
)
