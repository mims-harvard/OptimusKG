import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    drug_indication: pl.DataFrame,
    drug_disease: pl.DataFrame,
) -> pl.DataFrame:
    opentargets_drug_disease = (
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
                    pl.lit(["indication"]).alias(
                        "relationType"
                    ),  # TODO: replace this with strong/weak clinical evidence derived from highestClinicalTrialPhase
                ]
            ).alias("opentargets_properties"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )

    drugcentral_drug_disease = (
        drug_disease.select(
            pl.col("from"),
            pl.col("to"),
            pl.lit("drug_disease").alias("relation"),
            pl.lit(True).alias("undirected"),
            pl.struct(
                [
                    pl.lit(["drugcentral", "drugbank"]).alias("sources"),
                    pl.col("relationship_name")
                    .cast(pl.List(pl.Utf8))
                    .alias("relationType"),
                    pl.col("structure_id").alias("structureId"),
                    pl.col("drug_disease_id").alias("drugDiseaseId"),
                ]
            ).alias("drugcentral_properties"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )

    return (
        opentargets_drug_disease.join(
            drugcentral_drug_disease, on=["from", "to"], how="full"
        )
        .select(
            [
                pl.coalesce([pl.col("from"), pl.col("from_right")]).alias("from"),
                pl.coalesce([pl.col("to"), pl.col("to_right")]).alias("to"),
                pl.coalesce([pl.col("undirected"), pl.col("undirected_right")]).alias(
                    "undirected"
                ),
                pl.coalesce([pl.col("relation"), pl.col("relation_right")]).alias(
                    "relation"
                ),
                pl.struct(
                    [
                        pl.concat_list(
                            [
                                pl.coalesce(
                                    [
                                        pl.col("drugcentral_properties").struct.field(
                                            "relationType"
                                        ),
                                        pl.lit([], dtype=pl.List(pl.Utf8)),
                                    ]
                                ),
                                pl.coalesce(
                                    [
                                        pl.col("opentargets_properties").struct.field(
                                            "relationType"
                                        ),
                                        pl.lit([], dtype=pl.List(pl.Utf8)),
                                    ]
                                ),
                            ]
                        ).alias("relationType"),
                        pl.concat_list(
                            [
                                pl.coalesce(
                                    [
                                        pl.col("drugcentral_properties").struct.field(
                                            "sources"
                                        ),
                                        pl.lit([], dtype=pl.List(pl.Utf8)),
                                    ]
                                ),
                                pl.coalesce(
                                    [
                                        pl.col("opentargets_properties").struct.field(
                                            "sources"
                                        ),
                                        pl.lit([], dtype=pl.List(pl.Utf8)),
                                    ]
                                ),
                            ]
                        ).alias("sources"),
                        pl.col("drugcentral_properties").struct.field("structureId"),
                        pl.col("drugcentral_properties").struct.field("drugDiseaseId"),
                        pl.col("opentargets_properties").struct.field("referenceIds"),
                        pl.col("opentargets_properties").struct.field(
                            "highestClinicalTrialPhase"
                        ),
                    ]
                ).alias("properties"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


drug_disease_node = node(
    run,
    inputs={
        "drug_indication": "bronze.opentargets.drug_indication",
        "drug_disease": "bronze.drugcentral.drug_disease",
    },
    outputs="edges.drug_disease",
    name="drug_disease",
    tags=["silver"],
)
