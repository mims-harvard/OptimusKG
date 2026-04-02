import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    uberon: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    predicates = {
        "description": "http://purl.org/dc/elements/1.1/description",
        "title": "http://purl.org/dc/elements/1.1/title",
        "license": "http://purl.org/dc/terms/license",
        "version": "http://www.w3.org/2002/07/owl#versionInfo",
    }
    uberon_terms = (
        uberon.explode("graphs")
        .select(
            pl.col("graphs").struct.field("nodes").alias("nodes"),
            pl.col("graphs").struct.field("meta").alias("meta"),
        )
        .explode("nodes")
        .select(
            pl.col("nodes")
            .struct.field("id")
            .str.replace("http://purl.obolibrary.org/obo/", "")
            .alias("id"),
            pl.col("nodes").struct.field("lbl").alias("name"),
            pl.col("nodes").struct.field("meta").alias("meta"),
            pl.col("meta").struct.field("basicPropertyValues").alias("meta_bpv"),
        )
        .filter(pl.col("id").str.contains("UBERON_"))
        .with_columns(
            pl.col("meta")
            .struct.field("basicPropertyValues")
            .list.eval(
                pl.when(
                    pl.element().struct.field("pred")
                    == "http://www.geneontology.org/formats/oboInOwl#hasOBONamespace"
                ).then(pl.element().struct.field("val"))
            )
            .list.drop_nulls()
            .list.first()
            .alias("type"),
            pl.col("meta")
            .struct.field("definition")
            .struct.field("val")
            .alias("definition"),
            pl.col("meta")
            .struct.field("xrefs")
            .list.eval(pl.element().struct.field("val"))
            .alias("xrefs"),
            pl.col("meta")
            .struct.field("synonyms")
            .list.eval(pl.element().struct.field("val"))
            .alias("synonyms"),
        )
        .with_columns(pl.col("type").fill_null("CLASS"))
        .with_columns(
            pl.struct(
                [
                    pl.col("meta_bpv")
                    .list.eval(
                        pl.element()
                        .filter(pl.element().struct.field("pred") == uri)
                        .struct.field("val")
                    )
                    .list.get(0)
                    .alias(name)
                    for name, uri in predicates.items()
                ]
            ).alias("ontology")
        )
        .drop(["meta_bpv", "meta"])
        .unique()
        .select(
            [
                "id",
                "name",
                "type",
                "definition",
                "xrefs",
                "synonyms",
                "ontology",
            ]
        )
        .sort(by=["id", "name"])
    )

    uberon_relations = (
        uberon.explode("graphs")
        .select(
            pl.col("graphs").struct.field("edges"),
        )
        .explode("edges")
        .select(
            pl.col("edges")
            .struct.field("sub")
            .str.replace("http://purl.obolibrary.org/obo/", "")
            .alias("id"),
            pl.col("edges").struct.field("pred").alias("relation_type"),
            pl.col("edges")
            .struct.field("obj")
            .str.replace("http://purl.obolibrary.org/obo/", "")
            .alias("relation_id"),
        )
        .filter(
            pl.col("id").str.contains("UBERON_"),
            pl.col("relation_id").str.contains("UBERON_"),
            pl.col("relation_type").str.contains("is_a"),
        )
        .sort(["id", "relation_id"])
    )

    logger.info(
        f"Uberon: {uberon_terms.shape[0]} terms, {uberon_relations.shape[0]} relations"
    )

    return uberon_terms, uberon_relations


uberon_node = node(
    run,
    inputs={
        "uberon": "landing.ontology.uberon",
    },
    outputs=[
        "ontology.uberon_terms",
        "ontology.uberon_relations",
    ],
    name="uberon",
    tags=["bronze"],
)
