import polars as pl
from kedro.pipeline import node


def run(
    protein_protein: pl.DataFrame,
) -> pl.DataFrame:
    return (
        protein_protein.select(
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
        "protein_protein": "landing.ppi.protein_protein",
    },
    outputs="ppi.protein_protein",
    name="ppi",
    tags=["bronze"],
)
