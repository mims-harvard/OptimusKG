import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    hp: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    predicates = {
        "description": "http://purl.org/dc/elements/1.1/description",
        "title": "http://purl.org/dc/elements/1.1/title",
        "license": "http://purl.org/dc/terms/license",
        "version": "http://www.w3.org/2002/07/owl#versionInfo",
    }

    hp_terms = (
        hp.explode("graphs")
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
        .filter(pl.col("id").str.contains("HP_"))
        .with_columns(
            pl.lit("HP").alias("source"),
            pl.col("meta")
            .struct.field("definition")
            .struct.field("val")
            .alias("definition"),
            pl.col("meta")
            .struct.field("xrefs")
            .list.eval(pl.element().struct.field("val"))
            .list.join("|")
            .alias("xrefs"),
            pl.col("meta")
            .struct.field("synonyms")
            .list.eval(pl.element().struct.field("val"))
            .list.join("|")
            .alias("synonyms"),
        )
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
                "source",
                "definition",
                "xrefs",
                "synonyms",
                "ontology_description",
                "ontology_title",
                "ontology_license",
                "ontology_version",
            ]
        )
        .sort("id")
    )

    hp_xrefs = (
        hp_terms.select(["id", "xrefs"])
        .filter(pl.col("xrefs").is_not_null())
        .rename({"id": "hp_id"})
        .with_columns(pl.col("xrefs").str.split("|"))
        .explode("xrefs")
        .with_columns(pl.col("xrefs").str.split_exact(":", 1).alias("split_cols"))
        .unnest("split_cols")
        .rename({"field_0": "ontology", "field_1": "ontology_id"})
        .select(["hp_id", "ontology", "ontology_id"])
    )

    hp_relations = (
        hp.explode("graphs")
        .select(
            pl.col("graphs").struct.field("edges"),
        )
        .explode("edges")
        .select(
            pl.col("edges")
            .struct.field("obj")
            .str.replace("http://purl.obolibrary.org/obo/", "")
            .alias("parent"),
            pl.col("edges")
            .struct.field("sub")
            .str.replace("http://purl.obolibrary.org/obo/", "")
            .alias("child"),
        )
        .filter(
            pl.col("parent").str.contains("HP_"), pl.col("child").str.contains("HP_")
        )
    )

    return hp_terms, hp_xrefs, hp_relations


hp_node = node(
    run,
    inputs={
        "hp": "landing.ontology.hp",
    },
    outputs=[
        "ontology.hp_terms",
        "ontology.hp_xrefs",
        "ontology.hp_relations",
    ],
    name="phenotypes",
    tags=["bronze"],
)
