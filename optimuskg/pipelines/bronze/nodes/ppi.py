import polars as pl
from kedro.pipeline import node


def run(
    gene_gene: pl.DataFrame,
) -> pl.DataFrame:
    return (
        gene_gene.select(
            ("NCBIGene:" + pl.col("proteinA_entrezid")).alias("from"),
            ("NCBIGene:" + pl.col("proteinB_entrezid")).alias("to"),
            pl.col("databases").str.split("|").alias("databases"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


ppi_node = node(
    run,
    inputs={
        "gene_gene": "landing.ppi.protein_protein",
    },
    outputs="ppi.gene_gene",
    name="ppi",
    tags=["bronze"],
)
