import polars as pl
from kedro.pipeline import node


def run(  # noqa: PLR0913
    disease: pl.DataFrame,
    disease_disease: pl.DataFrame,
    exposure_disease: pl.DataFrame,
    drug_disease: pl.DataFrame,
    disease_protein: pl.DataFrame,
    disease_phenotype: pl.DataFrame,
) -> pl.DataFrame:
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
        .join(disease, left_on="id", right_on="id", how="inner")
        .unnest("metadata")
        .unnest("synonyms")
        .unnest("ontology")
        .select(
            pl.col("id"),
            pl.lit("disease").alias("node_type"),
            pl.struct(
                [
                    pl.col("id")
                    .str.extract(r"^([A-Za-z]+)_")
                    .alias(
                        "source"
                    ),  # NOTE: We keep disease from different ontologies, so we extract the source from the id
                    pl.col("name"),
                    pl.col("description"),
                    pl.col("code"),
                    pl.col("dbXRefs").alias("xrefs"),
                    pl.col("parents"),
                    pl.col("hasExactSynonym").alias("exactSynonyms"),
                    pl.col("hasRelatedSynonym").alias("relatedSynonyms"),
                    pl.col("hasNarrowSynonym").alias("narrowSynonyms"),
                    pl.col("hasBroadSynonym").alias("broadSynonyms"),
                    pl.col("obsoleteTerms"),
                    pl.col("obsoleteXRefs"),
                    pl.col("children"),
                    pl.col("ancestors"),
                    pl.col("descendants"),
                    pl.col("therapeuticAreas"),
                ]
            ).alias("properties"),
        )
        .sort(by="id")
    )


disease_node = node(
    run,
    inputs={
        "disease": "bronze.opentargets.disease",
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
