import polars as pl
from kedro.pipeline import node


def process_gene_names(
    protein_names: pl.DataFrame,
) -> pl.DataFrame:
    return protein_names


gene_names_node = node(
    process_gene_names,
    inputs={"protein_names": "bronze.gene_names.protein_names"},
    outputs="gene_names.protein_names",
    name="gene_names",
    tags=["silver"],
)
