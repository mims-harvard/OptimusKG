import polars as pl
from kedro.pipeline import node


def process_bgee(
    gene_expressions_in_anatomy: pl.DataFrame,
) -> pl.DataFrame:
    return gene_expressions_in_anatomy


bgee_node = node(
    process_bgee,
    inputs={"gene_expressions_in_anatomy": "bronze.bgee.gene_expressions_in_anatomy"},
    outputs="bgee.gene_expressions_in_anatomy",
    name="bgee",
    tags=["silver"],
)
