import polars as pl
from kedro.pipeline import node


def run(
    protein_names: pl.DataFrame,
    drug_protein: pl.DataFrame,
) -> pl.DataFrame:
    df_protein_drug = (
        drug_protein.join(
            protein_names, left_on="ncbi_gene_id", right_on="ncbi_id", how="left"
        )
        .rename(
            {
                "drug_bank_id": "x_id",
                "ncbi_gene_id": "y_id",
                "drug_bank_name": "x_name",
                "symbol": "y_name",
            }
        )
        .with_columns(
            [
                pl.lit("drug").alias("x_type"),
                pl.lit("DrugBank").alias("x_source"),
                pl.lit("gene").alias("y_type"),
                pl.lit("NCBI").alias("y_source"),
                pl.col("relation").alias("relation_type"),
                pl.lit("drug_protein").alias("relation"),
            ]
        )
    ).drop(["uniprot_id", "uniprot_name"])

    df_protein_drug = df_protein_drug.select(
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

    return df_protein_drug


drugbank_drug_protein_interactions_node = node(
    run,
    inputs={
        "protein_names": "bronze.gene_names.protein_names",
        "drug_protein": "bronze.drugbank.drug_protein",
    },
    outputs="drugbank.drug_protein",
    name="drugbank_drug_protein_interactions",
    tags=["silver"],
)
