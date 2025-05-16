import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.utils import clean_edges


def process_protein_cellular_component_interactions(
    ncbigene_protein_go_associations: pl.DataFrame,
    go_terms: pl.DataFrame,
    protein_names: pl.DataFrame,
) -> pl.DataFrame:
    # Join protein-GO associations with GO terms
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
            pl.lit("gene/protein").alias("x_type"),
            pl.lit("NCBI").alias("x_source"),
            pl.lit("GO").alias("y_source"),
        ]
    )

    # Select only the required columns
    df_prot_path = df_prot_path.select(
        ["x_id", "x_type", "x_name", "x_source", "y_id", "y_type", "y_name", "y_source"]
    )

    # Filter for cellular component and add relation information
    df_prot_cc = df_prot_path.filter(
        pl.col("y_type") == "cellular_component"
    ).with_columns(
        [
            pl.lit("cellcomp_protein").alias("relation"),
            pl.lit("interacts with").alias("display_relation"),
        ]
    )

    df_prot_cc = clean_edges(df_prot_cc)

    return df_prot_cc


protein_cellular_component_interactions_node = node(
    process_protein_cellular_component_interactions,
    inputs={
        "ncbigene_protein_go_associations": "bronze.ncbigene.protein_go_associations",
        "go_terms": "bronze.ontology.go_terms",
        "protein_names": "bronze.gene_names.protein_names",
    },
    outputs="ncbigene.protein_cellular_component_interactions",
    name="protein_cellular_component_interactions",
    tags=["silver"],
)
