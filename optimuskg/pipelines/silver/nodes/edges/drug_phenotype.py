import polars as pl
from kedro.pipeline import node


def run(
    high_confidence: pl.DataFrame,
    drug_indication: pl.DataFrame,
    drug_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    onsides_high_confidence = high_confidence.select(
        pl.col("ingredient_id").alias("from"),
        pl.col("effect_meddra_id").alias("to"),
        pl.lit("drug_phenotype").alias("relation"),
        pl.lit(True).alias("undirected"),
        pl.struct(
            [
                pl.lit(["OnSIDES"]).alias("sources"),
                pl.lit(None, dtype=pl.List(pl.Utf8)).alias("referenceIds"),
                pl.lit(None, dtype=pl.Float64).alias("highestClinicalTrialPhase"),
                pl.lit("adverse drug reaction").alias("relationType"),
                pl.lit(None, dtype=pl.Utf8).alias("structureId"),
                pl.lit(None, dtype=pl.Utf8).alias("drugDiseaseId"),
            ]
        ).alias("properties"),
    ).unique(subset=["from", "to"])

    phenotype_indication = (
        drug_indication.with_columns(
            pl.col("id").alias("drug_id"),
            pl.col("metadata").struct.field("indications"),
        )
        .explode("indications")
        .unnest("indications")
        .explode("references")
        .unnest("references")
        .filter(pl.col("disease").str.contains("HP"))
        .select(
            pl.col("drug_id").alias("from"),
            pl.col("disease").alias("to"),
            pl.lit("drug_phenotype").alias("relation"),
            pl.lit(True).alias("undirected"),
            pl.struct(
                [
                    pl.concat_list([pl.col("source")]).alias(
                        "sources"
                    ),  # transform source to list
                    pl.col("ids").alias("referenceIds"),
                    pl.col("maxPhaseForIndication").alias(
                        "highestClinicalTrialPhase"
                    ),  # TODO: convert opentargets number to actual string
                    pl.lit("associated with").alias(
                        "relationType"
                    ),  # TODO: the relationType should be inferred from the highestClinicalTrialPhase number
                    pl.lit(None, dtype=pl.Utf8).alias("structureId"),
                    pl.lit(None, dtype=pl.Utf8).alias("drugDiseaseId"),
                ]
            ).alias("properties"),
        )
        .unique(subset=["from", "to"])
    )

    drugcentral_drug_phenotype = drug_phenotype.select(
        pl.col("from"),
        pl.col("to"),
        pl.lit("drug_phenotype").alias("relation"),
        pl.lit(True).alias("undirected"),
        pl.struct(
            [
                pl.lit(["drugcentral"]).alias("sources"),
                pl.lit(None, dtype=pl.List(pl.Utf8)).alias("referenceIds"),
                pl.lit(None, dtype=pl.Float64).alias("highestClinicalTrialPhase"),
                pl.col("relationship_name").alias("relationType"),
                pl.col("structure_id").alias("structureId"),
                pl.col("drug_disease_id").alias("drugDiseaseId"),
            ]
        ).alias("properties"),
    ).unique(subset=["from", "to"])

    return (
        pl.concat(
            [onsides_high_confidence, phenotype_indication, drugcentral_drug_phenotype]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


drug_phenotype_node = node(
    run,
    inputs={
        "high_confidence": "bronze.onsides.high_confidence",
        "drug_indication": "bronze.opentargets.drug_indication",
        "drug_phenotype": "bronze.drugcentral.drug_phenotype",
    },
    outputs="edges.drug_phenotype",
    name="drug_phenotype",
    tags=["silver"],
)
