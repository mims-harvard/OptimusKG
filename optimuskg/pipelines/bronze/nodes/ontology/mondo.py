import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


OBO_PREFIX = "http://purl.obolibrary.org/obo/"


def _clean_uri(expr: pl.Expr) -> pl.Expr:
    """Removes the OBO prefix from a URI."""
    return expr.str.replace(OBO_PREFIX, "")


def process_mondo(
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

    mondo_terms = nodes_df.select(["id", "name", "type"]).unique()

    mondo_xrefs = (
        nodes_df.filter(pl.col("deprecated").is_null())
        .explode("xrefs")
        .select(
            pl.col("id"),
            pl.col("xrefs").struct.field("val").alias("xref_id"),
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
    process_mondo,
    inputs={
        "mondo": "landing.ontology.mondo",
    },
    outputs=[
        "ontology.mondo_terms",
        "ontology.mondo_relations",
        "ontology.mondo_xrefs",
        # "ontology.mondo_subsets",
        # "ontology.mondo_definitions",
    ],
    name="mondo",
    tags=["bronze"],
)
