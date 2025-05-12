import polars as pl
from kedro.pipeline import node


def process_gene_names(
    gene_names: pl.DataFrame,
) -> pl.DataFrame:
    return gene_names


gene_names_node = node(
    process_gene_names,
    inputs={"gene_names": "landing.gene_names.gene_names"},
    outputs="gene_names.gene_names",
    name="gene_names",
    tags=["bronze"],
)
