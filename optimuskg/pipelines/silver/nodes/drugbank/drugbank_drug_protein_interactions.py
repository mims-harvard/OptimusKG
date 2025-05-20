import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.utils import clean_edges


def process_drugbank_drug_protein_interactions(
    protein_names: pl.DataFrame,
    drug_protein: pl.DataFrame,
) -> pl.DataFrame:
    df_protein_drug = drug_protein.join(
        protein_names, left_on="ncbi_gene_id", right_on="ncbi_id", how="left"
    )

    df_protein_drug = df_protein_drug.rename(
        {
            "drug_bank": "x_id",
            "ncbi_gene_id": "y_id",
            "drug_bank_name": "x_name",
            "symbol": "y_name",
        }
    )

    df_protein_drug = df_protein_drug.with_columns(
        [
            pl.lit("drug").alias("x_type"),
            pl.lit("drugbank").alias("x_source"),
            pl.lit("gene/protein").alias("y_type"),
            pl.lit("ncbi").alias("y_source"),
            pl.col("relation").alias("display_relation"),
            pl.lit("drug_protein").alias("relation"),
        ]
    )

    df_protein_drug = clean_edges(df_protein_drug)

    return df_protein_drug


drugbank_drug_protein_interactions_node = node(
    process_drugbank_drug_protein_interactions,
    inputs={
        "protein_names": "bronze.gene_names.protein_names",
        "drug_protein": "bronze.drugbank.drug_protein",
    },
    outputs="drugbank.drug_protein",
    name="drugbank_drug_protein_interactions",
    tags=["silver"],
)
