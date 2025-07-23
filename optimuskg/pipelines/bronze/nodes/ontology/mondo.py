import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


OBO_PREFIX = "http://purl.obolibrary.org/obo/"


def _clean_uri(expr: pl.Expr) -> pl.Expr:
    """Removes the OBO prefix from a URI."""
    return expr.str.replace(OBO_PREFIX, "")


def run(
    mondo: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Processes the raw Mondo ontology data to extract terms, relations, and cross-references.

    Args:
        mondo: The raw Mondo ontology DataFrame from the landing zone.

    Returns:
        A tuple containing three DataFrames:
        - mondo_terms: Unique Mondo terms with their IDs, names, and types.
        - mondo_relations: Hierarchical 'is_a' relationships between Mondo terms.
        - mondo_xrefs: Cross-references for non-deprecated Mondo terms.
    """
    graphs = mondo.explode("graphs")

    predicates = {
        "description": "http://purl.org/dc/elements/1.1/description",
        "title": "http://purl.org/dc/elements/1.1/title",
        "license": "http://purl.org/dc/terms/license",
    }

    mondo_terms = (
        mondo.explode("graphs")
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
            pl.col("nodes").struct.field("type").alias("type"),
            pl.col("nodes").struct.field("meta").alias("meta"),
            pl.col("meta")
            .struct.field("version")
            .str.extract(r"(\d{4}-\d{2}-\d{2})", 1)
            .alias("ontology_version"),
            pl.col("meta").struct.field("basicPropertyValues").alias("meta_bpv"),
        )
        .filter(pl.col("id").str.contains("MONDO_"))
        .with_columns(
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
            *[
                pl.col("meta_bpv")
                .list.eval(
                    pl.element()
                    .filter(pl.element().struct.field("pred") == uri)
                    .struct.field("val")
                )
                .list.get(0)
                .alias(f"ontology_{name}")
                for name, uri in predicates.items()
            ],
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

    nodes_df = (
        graphs.select(pl.col("graphs").struct.field("nodes"))
        .explode("nodes")
        .select(
            [
                _clean_uri(pl.col("nodes").struct.field("id")).alias("id"),
                pl.col("nodes").struct.field("lbl").alias("name"),
                pl.col("nodes").struct.field("type").alias("type"),
                pl.col("nodes")
                .struct.field("meta")
                .struct.field("deprecated")
                .alias("deprecated"),
                pl.col("nodes")
                .struct.field("meta")
                .struct.field("xrefs")
                .alias("xrefs"),
            ]
        )
        .filter(pl.col("id").str.contains("MONDO_"))
    )

    mondo_xrefs = (
        nodes_df.filter(pl.col("deprecated").is_null())
        .explode("xrefs")
        .select(
            pl.col("id"),
            pl.col("xrefs").struct.field("val").alias("xref_id"),
        )
        .drop_nulls()
        .with_columns(
            pl.col("xref_id")
            .map_elements(
                lambda x: x.replace("Orphanet:", "Orphanet_"), return_dtype=pl.Utf8
            )
            .alias("xref_id")
        )
        .unique()
    )

    mondo_relations = (
        graphs.select(pl.col("graphs").struct.field("edges"))
        .explode("edges")
        .select(
            [
                _clean_uri(pl.col("edges").struct.field("sub")).alias("tail"),
                _clean_uri(pl.col("edges").struct.field("pred")).alias("edge_type"),
                _clean_uri(pl.col("edges").struct.field("obj")).alias("head"),
            ]
        )
        .filter(
            pl.col("tail").str.contains("MONDO_")
            & pl.col("head").str.contains("MONDO_")
            & (pl.col("edge_type") == "is_a")
        )
    )
    return mondo_terms, mondo_relations, mondo_xrefs


mondo_node = node(
    run,
    inputs={
        "mondo": "landing.ontology.mondo",
    },
    outputs=[
        "ontology.mondo_terms",
        "ontology.mondo_relations",
        "ontology.mondo_xrefs",
    ],
    name="mondo",
    tags=["bronze"],
)
