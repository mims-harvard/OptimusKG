import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def _handle_duplicate_edges(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(
            pl.concat_str(
                [
                    pl.col("x_index").cast(pl.Utf8),
                    pl.lit("_"),
                    pl.col("y_index").cast(pl.Utf8),
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
        .sort(["x_index", "y_index", "relation_order"])
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
    primekg_nodes: pl.DataFrame,
    primekg_edges: pl.DataFrame,
) -> pl.DataFrame:
    df = (
        pl.concat(
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
        .with_columns(pl.col("x_index").cast(pl.Utf8), pl.col("y_index").cast(pl.Utf8))
        .join(primekg_nodes, left_on="x_index", right_on="node_index", how="left")
        .rename(
            {
                "node_id": "x_id",
                "node_type": "x_type",
                "node_name": "x_name",
                "node_source": "x_source",
            }
        )
        .select(primekg_edges.columns)
        .unique()
        .pipe(_handle_duplicate_edges)
    )

    # Create reverse edges
    rev_edges = (
        df.clone()
        .rename(
            {
                "x_index": "y_index",
                "y_index": "x_index",
            }
        )
        .select(primekg_edges.columns)
    )

    # Merge with PrimeKG
    new_kg_edges = pl.concat([primekg_edges, df, rev_edges])

    # Final deduplication
    new_kg_edges = _handle_duplicate_edges(new_kg_edges)

    # Log statistics
    logger.debug(f"PrimeKG nodes: {primekg_nodes.height}")
    logger.debug(f"Final KG edges: {new_kg_edges.height}")

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
        "primekg_nodes": "landing.opentargets.primekg_nodes",
        "primekg_edges": "landing.opentargets.primekg_edges",
    },
    outputs="opentargets.opentargets_edges",
    name="opentargets_edges",
    tags=["silver"],
)
