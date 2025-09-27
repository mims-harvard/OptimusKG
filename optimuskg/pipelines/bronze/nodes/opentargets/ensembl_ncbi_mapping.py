import polars as pl
from kedro.pipeline import node


def run(
    protein_names: pl.DataFrame,
    target: pl.DataFrame,
) -> pl.DataFrame:
    return (
        target.select("id", "approved_symbol")
        .join(protein_names, left_on="approved_symbol", right_on="symbol", how="inner")
        .select(pl.col("id").alias("ensembl_id"), pl.col("ncbi_id"))
        .unique(subset=["ensembl_id", "ncbi_id"])
        .sort(by=["ensembl_id", "ncbi_id"])
    )


ensembl_ncbi_mapping_node = node(
    run,
    inputs={
        "protein_names": "bronze.gene_names.protein_names",
        "target": "bronze.opentargets.target",
    },
    outputs="opentargets.ensembl_ncbi_mapping",
    name="ensembl_ncbi_mapping",
    tags=["bronze"],
)
