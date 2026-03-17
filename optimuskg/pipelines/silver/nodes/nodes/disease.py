import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Node, Source


def run(  # noqa: PLR0913
    opentargets_disease: pl.DataFrame,
    mondo_terms: pl.DataFrame,
    drugcentral_disease: pl.DataFrame,
    disease_disease: pl.DataFrame,
    exposure_disease: pl.DataFrame,
    drug_disease: pl.DataFrame,
    disease_protein: pl.DataFrame,
    disease_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    non_disease_prefixes = ("GO_", "HP_", "UBERON_")
    return (
        pl.concat(
            [
                disease_disease.select(
                    pl.concat_list(["from", "to"]).explode().alias("id")
                ),
                disease_phenotype.select(pl.col("from").alias("id")),
                disease_protein.select(pl.col("from").alias("id")),
                drug_disease.select(pl.col("to").alias("id")),
                exposure_disease.select(pl.col("to").alias("id")),
            ]
        )
        .unique(subset="id")
        .filter(
            ~pl.col("id").str.starts_with(non_disease_prefixes[0])
            & ~pl.col("id").str.starts_with(non_disease_prefixes[1])
            & ~pl.col("id").str.starts_with(non_disease_prefixes[2])
        )
        .join(opentargets_disease, left_on="id", right_on="id", how="left")
        .unnest("metadata")
        .unnest("synonyms")
        .unnest("ontology")
        .join(  # NOTE: there are some MONDO diseases that are not present in the opentargets.disease table, so we join with the mondo_terms table
            mondo_terms.select(
                pl.col("id"),
                pl.col("name"),
                pl.col("definition").alias("description"),
                pl.col("xrefs"),
                pl.col("synonyms"),
            ),
            left_on="id",
            right_on="id",
            how="left",
        )
        .join(drugcentral_disease, left_on="id", right_on="id", how="left")
        .unique(subset="id")
        .select(
            pl.col("id"),
            pl.lit(Node.DISEASE).alias("label"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit(
                                [Source.OPEN_TARGETS, Source.DRUG_CENTRAL, Source.MONDO]
                            )
                            .cast(pl.List(pl.String))
                            .alias("direct"),
                            pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                        ]
                    ).alias("sources"),
                    pl.coalesce([pl.col("name"), pl.col("name_right")]).alias("name"),
                    pl.coalesce(
                        [pl.col("description"), pl.col("description_right")]
                    ).alias("description"),
                    pl.col("code"),
                    pl.concat_list([pl.col("xrefs"), pl.col("db_xrefs")])
                    .list.drop_nulls()
                    .list.unique()
                    .alias("xrefs"),
                    pl.col("parents"),
                    pl.concat_list([pl.col("has_exact_synonym"), pl.col("synonyms")])
                    .list.drop_nulls()
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
                    pl.col("therapeutic_areas").alias("therapeutic_areas"),
                    pl.col("leaf").alias("is_leaf"),
                    pl.col("concept_ids").alias("concept_ids"),
                    pl.col("concept_names").alias("concept_names"),
                    pl.col("umls_cui").alias("umls_cui"),
                    pl.col("snomed_full_names").alias("snomed_full_names"),
                    pl.col("cui_semantic_type").alias("cui_semantic_type"),
                    pl.col("snomed_conceptids").alias("snomed_concept_ids"),
                ]
            ).alias("properties"),
        )
    ).sort(by="id")


disease_node = node(
    run,
    inputs={
        "opentargets_disease": "bronze.opentargets.disease",
        "mondo_terms": "bronze.ontology.mondo_terms",
        "drugcentral_disease": "bronze.drugcentral.disease",
        "disease_disease": "silver.edges.disease_disease",
        "exposure_disease": "silver.edges.exposure_disease",
        "drug_disease": "silver.edges.drug_disease",
        "disease_protein": "silver.edges.disease_protein",
        "disease_phenotype": "silver.edges.disease_phenotype",
    },
    outputs="nodes.disease",
    name="disease",
    tags=["silver"],
)
