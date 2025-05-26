import logging

import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.utils import clean_edges

logger = logging.getLogger(__name__)


def process_protein_biological_process_interactions(
    ncbigene_protein_go_associations: pl.DataFrame,
    go_terms: pl.DataFrame,
    protein_names: pl.DataFrame,
) -> pl.DataFrame:
    df_prot_path = ncbigene_protein_go_associations.join(
        go_terms, left_on="go_term_id", right_on="id", how="inner"
    )

    df_prot_path = df_prot_path.join(
        protein_names, left_on="ncbi_gene_id", right_on="ncbi_id", how="left"
    )

    # Rename columns to match the expected format
    df_prot_path = df_prot_path.rename(
        {
            "ncbi_gene_id": "x_id",
            "symbol": "x_name",
            "go_term_id": "y_id",
            "name": "y_name",
            "go_term_type": "y_type",
        }
    )

    # Add constant columns
    df_prot_path = df_prot_path.with_columns(
        [
            pl.lit("gene").alias("x_type"),
            pl.lit("NCBI").alias("x_source"),
            pl.lit("GO").alias("y_source"),
        ]
    )

    # Select only the required columns
    df_prot_path = df_prot_path.select(
        ["x_id", "x_type", "x_name", "x_source", "y_id", "y_type", "y_name", "y_source"]
    )

    # Filter for biological process and add relation information
    df_prot_bp = df_prot_path.filter(
        pl.col("y_type") == "biological_process"
    ).with_columns(
        [
            pl.lit("biological_process_protein").alias("relation"),
            pl.lit("interacts with").alias("display_relation"),
        ]
    )

    df_prot_bp = clean_edges(df_prot_bp)

    return df_prot_bp


protein_biological_process_interactions_node = node(
    process_protein_biological_process_interactions,
    inputs={
        "ncbigene_protein_go_associations": "bronze.ncbigene.protein_go_associations",
        "go_terms": "bronze.ontology.go_terms",
        "protein_names": "bronze.gene_names.protein_names",
    },
    outputs="ncbigene.protein_biological_process_interactions",
    name="protein_biological_process_interactions",
    tags=["silver"],
)
