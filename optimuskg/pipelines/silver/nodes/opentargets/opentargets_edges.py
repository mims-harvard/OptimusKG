import logging

import polars as pl
from kedro.pipeline import node
from itertools import combinations

logger = logging.getLogger(__name__)


def _handle_duplicate_edges(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(
            pl.concat_str(
                [
                    pl.col("x_id").cast(pl.Utf8),
                    pl.lit("_"),
                    pl.col("y_id").cast(pl.Utf8),
                ]
            ).alias("edge_id")
        )
        .with_columns(
            is_duplicate=pl.col("edge_id").is_duplicated(),
            relation_order=pl.col("relation").replace_strict(
                {
                    "disease_protein_positive": 0,
                    "disease_protein_negative": 1,
                    "disease_protein": 2,
                },
                default="unknown",
            ),
        )
        .sort(["x_id", "y_id", "relation_order"])
        .filter(
            ~pl.col("is_duplicate")
            | (pl.col("edge_id") == pl.col("edge_id").first().over("edge_id"))
        )
        .drop(["edge_id", "is_duplicate", "relation_order"])
    )


def process_opentargets_edges(  # noqa: PLR0913
    cancer_gene_census: pl.DataFrame,
    chembl_drug_disease: pl.DataFrame,
    chembl_drug_gene: pl.DataFrame,
    clingen: pl.DataFrame,
    crispr: pl.DataFrame,
    crispr_screen: pl.DataFrame,
    expression_atlas: pl.DataFrame,
    gene_burden: pl.DataFrame,
    gene2phenotype: pl.DataFrame,
    genomics_england: pl.DataFrame,
    intogen: pl.DataFrame,
    progeny: pl.DataFrame,
    reactome: pl.DataFrame,
    slapenrich: pl.DataFrame,
    sysbio: pl.DataFrame,
    uniprot_literature: pl.DataFrame,
    orphanet: pl.DataFrame,
) -> pl.DataFrame:
    df = pl.concat(
        [
            cancer_gene_census,
            chembl_drug_disease,
            chembl_drug_gene,
            clingen,
            crispr,
            crispr_screen,
            expression_atlas,
            gene_burden,
            gene2phenotype,
            genomics_england,
            intogen,
            orphanet,
            progeny,
            reactome,
            slapenrich,
            sysbio,
            uniprot_literature,
        ],
        how="diagonal",
    )

    df = df.unique().pipe(_handle_duplicate_edges)

    # Create reverse edges
    rev_edges = (
        df.clone()
        .rename(
            {
                "x_id": "y_id",
                "x_type": "y_type",
                "y_id": "x_id",
                "y_type": "x_type",
            }
        )
        .select(['x_id', 'y_id', 'relation', 'display_relation', 'x_type', 'y_type'])
    )


    new_kg_edges = pl.concat([df, rev_edges])

    # Final deduplication
    new_kg_edges = _handle_duplicate_edges(new_kg_edges)

    # Log statistics
    logger.debug(f"Final KG edges: {new_kg_edges.height}")

    # Add blank columns for x_type, x_name, x_source, y_type, y_name, y_source all at once
    new_kg_edges = new_kg_edges.with_columns(
        [
            pl.lit("___").alias("x_name"),
            pl.lit("___").alias("x_source"),
            pl.lit("___").alias("y_name"),
            pl.lit("___").alias("y_source"),
        ]
    )

    return new_kg_edges


opentargets_edges_node = node(
    process_opentargets_edges,
    inputs={
        "cancer_gene_census": "opentargets.evidence.cancer_gene_census",
        "chembl_drug_disease": "opentargets.evidence.chembl_drug_disease",
        "chembl_drug_gene": "opentargets.evidence.chembl_drug_gene",
        "clingen": "opentargets.evidence.clingen",
        "crispr": "opentargets.evidence.crispr",
        "crispr_screen": "opentargets.evidence.crispr_screen",
        "expression_atlas": "opentargets.evidence.expression_atlas",
        "gene_burden": "opentargets.evidence.gene_burden",
        "gene2phenotype": "opentargets.evidence.gene2phenotype",
        "genomics_england": "opentargets.evidence.genomics_england",
        "intogen": "opentargets.evidence.intogen",
        "progeny": "opentargets.evidence.progeny",
        "reactome": "opentargets.evidence.reactome",
        "slapenrich": "opentargets.evidence.slapenrich",
        "sysbio": "opentargets.evidence.sysbio",
        "uniprot_literature": "opentargets.evidence.uniprot_literature",
        "orphanet": "opentargets.evidence.orphanet",
    },
    outputs="opentargets.opentargets_edges",
    name="opentargets_edges",
    tags=["silver"],
)
