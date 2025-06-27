import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(  # noqa: PLR0913
    go_plus: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    go_terms = (
        go_plus.explode("graphs")
        .select(pl.col("graphs").struct.field("nodes"))
        .explode("nodes")
        .select(
            [
                pl.col("nodes")
                .struct.field("id")
                .str.replace("http://purl.obolibrary.org/obo/", "")
                .alias("id"),
                pl.col("nodes").struct.field("lbl").alias("name"),
                pl.col("nodes")
                .struct.field("meta")
                .struct.field("basicPropertyValues")
                .alias("bpv"),
            ]
        )
        .filter(pl.col("id").str.contains("GO_"))
        .with_columns(
            pl.col("bpv")
            .list.eval(
                pl.when(
                    pl.element().struct.field("pred")
                    == "http://www.geneontology.org/formats/oboInOwl#hasOBONamespace"
                ).then(pl.element().struct.field("val"))
            )
            .list.drop_nulls()
            .list.first()
            .alias("type")
        )
        .drop("bpv")
        .with_columns(pl.col("type").fill_null("CLASS"))
        .unique()
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
