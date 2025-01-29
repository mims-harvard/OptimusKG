import logging

import polars as pl
from kedro.pipeline import node

log = logging.getLogger(__name__)


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
    open_targets_edges = pl.concat(
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

    open_targets_edges = open_targets_edges.with_columns(
        pl.col("x_index").cast(pl.Utf8).alias("x_index")
    )

    open_targets_edges = open_targets_edges.join(
        primekg_nodes, left_on="x_index", right_on="node_index", how="left"
    ).rename(
        {
            "node_id": "x_id",
            "node_type": "x_type",
            "node_name": "x_name",
            "node_source": "x_source",
        }
    )

    open_targets_edges = open_targets_edges.with_columns(
        pl.col("y_index").cast(pl.Utf8).alias("y_index")
    )

    open_targets_edges = open_targets_edges.join(
        primekg_nodes, left_on="y_index", right_on="node_index", how="left"
    ).rename(
        {
            "node_id": "y_id",
            "node_type": "y_type",
            "node_name": "y_name",
            "node_source": "y_source",
        }
    )

    log.info(open_targets_edges.schema)
    log.info(primekg_edges.columns)

    open_targets_edges = open_targets_edges.select(primekg_edges.columns).unique()

    log.info(open_targets_edges.schema)

    # Find and remove duplicates
    open_targets_edges = open_targets_edges.with_columns(
        pl.concat_str(
            [
                pl.col("x_index").cast(pl.Utf8),
                pl.lit("_"),
                pl.col("y_index").cast(pl.Utf8),
            ]
        ).alias("dup")
    )

    dup_list = (
        open_targets_edges.filter(pl.col("dup").is_duplicated())
        .get_column("dup")
        .unique()
    )

    # Subset duplicates
    dup_edges = open_targets_edges.filter(pl.col("dup").is_in(dup_list)).sort(
        ["x_index", "y_index", "relation"]
    )

    # Remove from original graph
    open_targets_edges = open_targets_edges.filter(~pl.col("dup").is_in(dup_list))

    dup_edges = (
        dup_edges.with_columns(
            pl.when(pl.col("relation") == "disease_protein_positive")
            .then(0)
            .when(pl.col("relation") == "disease_protein_negative")
            .then(1)
            .when(pl.col("relation") == "disease_protein")
            .then(2)
            .alias("relation_order")
        )
        .sort(["x_index", "y_index", "relation_order"])
        .unique(subset="dup")
        .drop("relation_order")
    )

    # Add back duplicated edges
    open_targets_edges = pl.concat([open_targets_edges, dup_edges])
    open_targets_edges = open_targets_edges.drop("dup")

    # Add to PrimeKG
    # Create reverse edges
    rev_edges = open_targets_edges.clone()
    rev_cols = {
        "x_index": "y_index",
        "x_id": "y_id",
        "x_type": "y_type",
        "x_name": "y_name",
        "x_source": "y_source",
        "y_index": "x_index",
        "y_id": "x_id",
        "y_type": "x_type",
        "y_name": "x_name",
        "y_source": "x_source",
    }
    rev_edges = rev_edges.rename(rev_cols)
    rev_edges = rev_edges.select(primekg_edges.columns)

    # Merge with PrimeKG
    open_targets_edges = open_targets_edges.cast({"y_index": pl.Float64})
    rev_edges = rev_edges.cast({"y_index": pl.Float64})
    new_kg_edges = pl.concat([primekg_edges, open_targets_edges, rev_edges])

    # Check for duplicates
    new_kg_edges = new_kg_edges.with_columns(
        pl.concat_str(
            [
                pl.col("x_index").cast(pl.Utf8),
                pl.lit("_"),
                pl.col("y_index").cast(pl.Utf8),
            ]
        ).alias("dup")
    )

    dup_list = (
        new_kg_edges.filter(pl.col("dup").is_duplicated()).get_column("dup").unique()
    )

    dup_edges = new_kg_edges.filter(pl.col("dup").is_in(dup_list)).sort(
        ["x_index", "y_index"]
    )

    # Remove duplicates
    new_kg_edges = new_kg_edges.filter(~pl.col("dup").is_in(dup_list))
    undup_edges = dup_edges.unique()
    new_kg_edges = pl.concat([new_kg_edges, undup_edges])
    new_kg_edges = new_kg_edges.drop("dup")

    log.info(f"KG nodes: {primekg_nodes.height}")
    log.info(f"KG edges: {new_kg_edges.height}")
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
        # NOTE: In the future, we should only use datasets from the previous or same zone.
        "primekg_nodes": "landing.opentargets.primekg_nodes",
        "primekg_edges": "landing.opentargets.primekg_edges",
    },
    outputs="opentargets.opentargets_edges",
    name="opentargets_edges",
)
