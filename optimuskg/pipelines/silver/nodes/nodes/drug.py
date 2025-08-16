import polars as pl
from kedro.pipeline import node


def run(  # noqa: PLR0913
    onsides_high_confidence: pl.DataFrame,
    drugbank_vocabulary: pl.DataFrame,
    drug_molecule: pl.DataFrame,
    drug_drug: pl.DataFrame,
    drug_protein: pl.DataFrame,
    drug_disease: pl.DataFrame,
    drug_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                drug_drug.with_columns(
                    [pl.col("properties").struct.field("sources").alias("sources")]
                )
                .unpivot(index=["sources"], on=["from", "to"], value_name="id")
                .select(["id", "sources"]),
                drug_disease.select(
                    [
                        pl.col("from").alias("id"),
                        pl.col("properties").struct.field("sources").alias("sources"),
                    ]
                ),
                drug_phenotype.select(
                    [
                        pl.col("from").alias("id"),
                        pl.col("properties").struct.field("sources").alias("sources"),
                    ]
                ),
                drug_protein.select(
                    [
                        pl.col("from").alias("id"),
                        pl.col("properties").struct.field("sources").alias("sources"),
                    ]
                ),
            ]
        )
        .unique(subset="id")
        .join(
            onsides_high_confidence.select(
                [
                    "ingredient_id",
                    "rxnorm_name",
                    "rxnorm_term_type",
                    pl.lit(["ONSIDES"]).alias("sources"),
                ]
            ).unique(subset="ingredient_id"),
            left_on="id",
            right_on="ingredient_id",
            how="left",
        )
        .join(drugbank_vocabulary, left_on="id", right_on="drugbank_id", how="left")
        .join(drug_molecule.unnest("metadata"), left_on="id", right_on="id", how="left")
        .with_columns(
            [
                pl.when(pl.col("synonyms").is_not_null() & (pl.col("synonyms") != ""))
                .then(pl.col("synonyms").str.split(" | "))
                .otherwise(None)
                .alias("synonyms_processed"),
                pl.when(
                    pl.col("accession_numbers").is_not_null()
                    & (pl.col("accession_numbers") != "")
                )
                .then(pl.col("accession_numbers").str.split(" | "))
                .otherwise(None)
                .alias("accession_processed"),
            ]
        )
        .select(
            [
                pl.col("id"),
                pl.lit("drug").alias("node_type"),
                pl.struct(
                    [
                        pl.coalesce(
                            [
                                pl.col("rxnorm_name"),
                                pl.col("common_name"),
                                pl.col("name"),
                            ]
                        ).alias("name"),
                        pl.coalesce(
                            [pl.col("standard_inchi_key"), pl.col("inchiKey")]
                        ).alias("inchiKey"),
                        pl.coalesce(
                            [pl.col("rxnorm_term_type"), pl.col("drugType")]
                        ).alias("type"),
                        pl.coalesce(
                            [pl.col("synonyms_processed"), pl.col("synonyms_right")]
                        ).alias("synonyms"),
                        pl.col("description"),
                        pl.col("accession_processed").alias("accessionNumbers"),
                        pl.col("canonicalSmiles"),
                        pl.col("cas").alias("chemicalAbstractsServiceNumber"),
                        pl.col("unii").alias("uniqueIngredientIdentifier"),
                        pl.col("blackBoxWarning"),
                        pl.col("yearOfFirstApproval"),
                        pl.col("maximumClinicalTrialPhase"),
                        pl.col("hasBeenWithdrawn"),
                        pl.col("isApproved"),
                        pl.when(pl.col("tradeNames").list.len() > 0)
                        .then(pl.col("tradeNames"))
                        .otherwise(None)
                        .alias("tradeNames"),
                        pl.coalesce(
                            [
                                pl.col("sources"),
                                pl.col("crossReferences").list.eval(
                                    pl.element().struct.field("source")
                                ),
                            ]
                        )
                        .list.unique()
                        .alias("sources"),
                        pl.col("crossReferences")
                        .list.eval(pl.element().struct.field("ids"))
                        .list.eval(pl.element().explode())
                        .alias("sourceIds"),
                    ]
                ).alias("properties"),
            ]
        )
        .sort(by="id")
    )

    # TODO: there are 2,699 drugs that only contains the column id but no properties.
    # This is because they are defined as "parent" relationships in the drug_drug dataset, but not present
    # in the drug_molecule dataset. You can verify this by filtering for: filter(pl.col("name").is_null())


drug_node = node(
    run,
    inputs={
        "onsides_high_confidence": "bronze.onsides.high_confidence",
        "drugbank_vocabulary": "bronze.drugbank.vocabulary",
        "drug_molecule": "bronze.opentargets.drug_molecule",
        "drug_drug": "silver.edges.drug_drug",
        "drug_protein": "silver.edges.drug_protein",
        "drug_disease": "silver.edges.drug_disease",
        "drug_phenotype": "silver.edges.drug_phenotype",
    },
    outputs="nodes.drug",
    name="drug",
    tags=["silver"],
)
