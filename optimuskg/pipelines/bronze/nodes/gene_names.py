import polars as pl
from kedro.pipeline import node


def process_gene_names(
    gene_names: pl.DataFrame,
) -> pl.DataFrame:
    # Rename columns to standardized names
    df_protein_names = gene_names.rename(
        {
            "NCBI Gene ID(supplied by NCBI)": "ncbi_id",
            "Approved symbol": "symbol",
        }
    )
    df_protein_names = df_protein_names.select(["ncbi_id", "symbol"]).drop_nulls()
    return df_protein_names


gene_names_node = node(
    process_gene_names,
    inputs={"gene_names": "landing.gene_names.gene_names"},
    outputs="gene_names.protein_names",
    name="gene_names",
    tags=["bronze"],
)
