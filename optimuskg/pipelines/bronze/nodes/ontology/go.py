import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(  # noqa: PLR0913
    go_plus: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    predicates = {
        "description": "http://purl.org/dc/elements/1.1/description",
        "title": "http://purl.org/dc/elements/1.1/title",
        "license": "http://purl.org/dc/terms/license",
        "version": "http://www.w3.org/2002/07/owl#versionInfo",
    }

    go_terms = (
        go_plus.explode("graphs")
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
        .filter(pl.col("id").str.contains("GO_"))
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
            .struct.field("definition")
            .struct.field("xrefs")
            .list.join("|")
            .alias("xrefs"),
            pl.col("meta")
            .struct.field("synonyms")
            .list.eval(pl.element().struct.field("val"))
            .list.join("|")
            .alias("synonyms"),
        )
        .with_columns(pl.col("type").fill_null("CLASS"))
        .with_columns(
            [
                pl.col("meta_bpv")
                .list.eval(
                    pl.element()
                    .filter(pl.element().struct.field("pred") == uri)
                    .struct.field("val")
                )
                .list.get(0)
                .alias(f"ontology_{name}")
                for name, uri in predicates.items()
            ]
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
                "ontology_description",
                "ontology_title",
                "ontology_license",
                "ontology_version",
            ]
        )
    )

    go_relations = (
        go_plus.explode("graphs")
        .select(pl.col("graphs").struct.field("edges"))
        .explode("edges")
        .select(
            [
                pl.col("edges")
                .struct.field("sub")
                .str.replace("http://purl.obolibrary.org/obo/", "")
                .alias("tail"),
                pl.col("edges")
                .struct.field("pred")
                .str.replace("http://purl.obolibrary.org/obo/", "")
                .alias("edge_type"),
                pl.col("edges")
                .struct.field("obj")
                .str.replace("http://purl.obolibrary.org/obo/", "")
                .alias("head"),
            ]
        )
        .filter(pl.col("tail").str.contains("GO_") & pl.col("head").str.contains("GO_"))
    )

    return go_terms, go_relations


go_plus_node = node(
    run,
    inputs={
        "go_plus": "landing.ontology.go_plus",
    },
    outputs=["ontology.go_terms", "ontology.go_relations"],
    name="go_plus",
    tags=["bronze"],
)
