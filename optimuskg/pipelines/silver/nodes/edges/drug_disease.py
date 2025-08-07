import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    drug_indication: pl.DataFrame,
    # TODO: research of adding this other drugbankdfs to the data
    # drug_disease: pl.DataFrame,
    # umls_mondo: pl.DataFrame,
    # mondo_terms: pl.DataFrame,
    # vocabulary: pl.DataFrame,
) -> pl.DataFrame:
    return (
        drug_indication.with_columns(
            pl.col("id"),
            pl.col("metadata").struct.field("indications"),
        )
        .explode("indications")
        .unnest("indications")
        .explode("references")
        .unnest("references")
        .filter(~pl.col("disease").str.contains("HP"))
        .select(
            pl.col("id").alias("from"),
            pl.col("disease").alias("to"),
            pl.lit("drug_disease").alias("relation"),
            pl.lit(True).alias("undirected"),
            pl.struct(
                [
                    pl.col("ids").alias("referenceIds"),
                    pl.concat_list([pl.col("source")]).alias(
                        "sources"
                    ),  # transform source to list
                    pl.col("maxPhaseForIndication").alias("highestClinicalTrialPhase"),
                    pl.lit("indication").alias(
                        "relationType"
                    ),  # TODO: replace this with strong/weak clinical evidence derived from highestClinicalTrialPhase
                ]
            ).alias("properties"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


drug_disease_node = node(
    run,
    inputs={
        "drug_indication": "bronze.opentargets.drug_indication",
    },
    outputs="edges.drug_disease",
    name="drug_disease",
    tags=["silver"],
)
