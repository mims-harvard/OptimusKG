import polars as pl
from kedro.pipeline import node


def run(
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

    # Add source, type, relation and relation_type columns
    df_path_prot = df_path_prot.with_columns(
        [
            pl.lit("NCBI").alias("x_source"),
            pl.lit("gene").alias("x_type"),
            pl.lit("REACTOME").alias("y_source"),
            pl.lit("pathway").alias("y_type"),
            pl.lit("pathway_protein").alias("relation"),
            pl.lit("interacts with").alias("relation_type"),
        ]
    )

    df_path_prot = df_path_prot.select(
        [
            "relation",
            "relation_type",
            "x_id",
            "x_type",
            "x_name",
            "x_source",
            "y_id",
            "y_type",
            "y_name",
            "y_source",
        ]
    )

    return df_path_prot


pathway_protein_node = node(
    run,
    inputs={
        "reactome_ncbi": "bronze.reactome.reactome_ncbi",
        "protein_names": "bronze.gene_names.protein_names",
    },
    outputs="pathway_protein",
    name="pathway_protein",
    tags=["silver"],
)
