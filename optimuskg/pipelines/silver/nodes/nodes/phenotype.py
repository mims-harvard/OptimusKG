import polars as pl
from kedro.pipeline import node


def run(  # noqa: PLR0913
    disease: pl.DataFrame,
    phenotype_protein: pl.DataFrame,
    disease_phenotype: pl.DataFrame,
    phenotype_phenotype: pl.DataFrame,
    drug_phenotype: pl.DataFrame,
    hp_terms: pl.DataFrame,
    vocab_meddra_adverse_effect: pl.DataFrame,
    drugcentral_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                disease_phenotype.select(pl.col("to").alias("id")),
                drug_phenotype.select(pl.col("to").alias("id")),
                phenotype_protein.select(pl.col("from").alias("id")),
                phenotype_phenotype.select(
                    pl.concat_list(["from", "to"]).explode().alias("id")
                ),
            ]
        )
        .unique(subset="id")
        .join(disease, left_on="id", right_on="id", how="left")
        .unnest("metadata")
        .unnest("synonyms")
        .unnest("ontology")
        .join(
            hp_terms,
            left_on="id",
            right_on="id",
            how="left",
        )
        .join(
            vocab_meddra_adverse_effect.with_columns(
                ("meddra:" + pl.col("meddra_id")).alias("id")
            ),
            left_on="id",
            right_on="id",
            how="left",
        )
        .join(drugcentral_phenotype, left_on="id", right_on="id", how="left")
        .select(
            pl.col("id"),
            pl.lit("phenotype").alias("label"),
            pl.struct(
                [
                    pl.lit(["MedDRA", "opentargets", "HP"]).alias("sources"),
                    pl.coalesce(
                        [pl.col("name"), pl.col("name_right"), pl.col("meddra_name")]
                    ).alias("name"),
                    pl.coalesce([pl.col("description"), pl.col("definition")]).alias(
                        "description"
                    ),
                    pl.col("code"),
                    pl.concat_list([pl.col("db_xrefs"), pl.col("xrefs").str.split("|")])
                    .list.unique()
                    .alias("xrefs"),
                    pl.col("parents"),
                    pl.concat_list(
                        [pl.col("has_exact_synonym"), pl.col("synonyms").str.split("|")]
                    )
                    .list.unique()
                    .alias("exact_synonyms"),
                    pl.col("has_related_synonym").alias("related_synonyms"),
                    pl.col("has_narrow_synonym").alias("narrow_synonyms"),
                    pl.col("has_broad_synonym").alias("broad_synonyms"),
                    pl.col("obsolete_terms").alias("obsolete_terms"),
                    pl.col("obsolete_xrefs").alias("obsolete_xrefs"),
                    pl.col("children"),
                    pl.col("ancestors"),
                    pl.col("descendants"),
                    pl.coalesce(
                        [pl.col("meddra_term_type"), pl.lit("phenotype")]
                    ).alias("type"),
                    pl.col("ontology_description").alias("ontology_description"),
                    pl.col("ontology_title").alias("ontology_title"),
                    pl.col("ontology_license").alias("ontology_license"),
                    pl.col("ontology_version").alias("ontology_version"),
                    pl.col("concept_ids").alias("concept_ids"),
                    pl.col("concept_names").alias("concept_names"),
                    pl.col("umls_cui").alias("umls_cui"),
                    pl.col("snomed_full_names").alias("snomed_full_names"),
                    pl.col("cui_semantic_type").alias("cui_semantic_type"),
                    pl.col("snomed_conceptids").alias("snomed_concept_ids"),
                ]
            ).alias("properties"),
        )
        .unique(subset="id")
        .sort(by="id")
    )


phenotype_node = node(
    run,
    inputs={
        "disease": "bronze.opentargets.disease",
        "phenotype_protein": "silver.edges.phenotype_protein",
        "disease_phenotype": "silver.edges.disease_phenotype",
        "phenotype_phenotype": "silver.edges.phenotype_phenotype",
        "drug_phenotype": "silver.edges.drug_phenotype",
        "hp_terms": "bronze.ontology.hp_terms",
        "vocab_meddra_adverse_effect": "landing.onsides.vocab_meddra_adverse_effect",  # TODO: transform this into a more-processed bronze dataset (e.g. change the "type" to an expanded text)
        "drugcentral_phenotype": "bronze.drugcentral.phenotype",
    },
    outputs="nodes.phenotype",
    name="phenotype",
    tags=["silver"],
)
