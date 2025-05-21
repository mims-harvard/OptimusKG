import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.utils import clean_edges


def process_pathway_protein_interactions(
    reactome_ncbi: pl.DataFrame,
    protein_names: pl.DataFrame,
) -> pl.DataFrame:
    df_path_prot = reactome_ncbi.join(
        protein_names, left_on="ncbi_id", right_on="ncbi_id", how="inner"
    )

    # Rename columns to match the expected format
    df_path_prot = df_path_prot.rename(
        {
            "ncbi_id": "x_id",
            "symbol": "x_name",
            "reactome_id": "y_id",
            "reactome_name": "y_name",
        }
    )

    # Add source, type, relation and display_relation columns
    df_path_prot = df_path_prot.with_columns(
        [
            pl.lit("NCBI").alias("x_source"),
            pl.lit("gene/protein").alias("x_type"),
            pl.lit("REACTOME").alias("y_source"),
            pl.lit("pathway").alias("y_type"),
            pl.lit("pathway_protein").alias("relation"),
            pl.lit("interacts with").alias("display_relation"),
        ]
    )

    # Clean edges
    df_path_prot = clean_edges(df_path_prot)

    return df_path_prot


pathway_protein_interactions_node = node(
    process_pathway_protein_interactions,
    inputs={
        "reactome_ncbi": "bronze.reactome.reactome_ncbi",
        "protein_names": "bronze.gene_names.protein_names",
    },
    outputs="reactome.pathway_protein_interactions",
    name="pathway_protein_interactions",
    tags=["silver"],
)
