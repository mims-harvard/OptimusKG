import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    disease: pl.DataFrame,
) -> pl.DataFrame:
    return (
        disease.select(
            pl.col("id"),
            pl.col("name"),
            pl.col("description"),
            pl.struct(
                pl.col("code"),
                pl.col("dbXRefs").alias("db_xrefs"),
                pl.col("parents"),
                pl.struct(
                    pl.col("synonyms")
                    .struct.field("hasExactSynonym")
                    .alias("has_exact_synonym"),
                    pl.col("synonyms")
                    .struct.field("hasRelatedSynonym")
                    .alias("has_related_synonym"),
                    pl.col("synonyms")
                    .struct.field("hasNarrowSynonym")
                    .alias("has_narrow_synonym"),
                    pl.col("synonyms")
                    .struct.field("hasBroadSynonym")
                    .alias("has_broad_synonym"),
                ).alias("synonyms"),
                pl.col("obsoleteTerms").alias("obsolete_terms"),
                pl.col("obsoleteXRefs").alias("obsolete_xrefs"),
                pl.col("children"),
                pl.col("ancestors"),
                pl.col("therapeuticAreas").alias("therapeutic_areas"),
                pl.col("descendants"),
                pl.struct(
                    pl.col("ontology")
                    .struct.field("isTherapeuticArea")
                    .alias("is_therapeutic_area"),
                    pl.col("ontology").struct.field("leaf"),
                    pl.col("ontology").struct.field("sources"),
                ).alias("ontology"),
            ).alias("metadata"),
        )
        .unique(
            subset=["name"]
        )  # TODO: There are diseases from different ontologies that refer to the same disease, like MONDO_0019402 and Orphanet_848.
        .sort(by=["id", "name"])
    )


disease_node = node(
    run,
    inputs={
        "disease": "landing.opentargets.disease",
    },
    outputs="opentargets.disease",
    name="disease",
    tags=["bronze"],
)
