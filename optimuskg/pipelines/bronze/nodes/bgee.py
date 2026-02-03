import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    homo_sapiens_expressions_advanced: pl.DataFrame,
) -> pl.DataFrame:
    df = homo_sapiens_expressions_advanced.filter(
        pl.col("Anatomical entity ID").str.starts_with("UBERON")
    )

    df = df.select(  # TODO: add more metadata with more columns from the landing version of gene_expressions_in_anatomy
        [
            pl.col("Gene ID").alias("gene_id"),
            pl.col("Gene name").alias("gene_name"),
            pl.col("Anatomical entity ID").alias("anatomy_id"),
            pl.col("Anatomical entity name").alias("anatomy_name"),
            pl.col("Expression").alias("expression"),
            pl.col("Call quality").alias("call_quality"),
            pl.col("Expression rank").alias("expression_rank"),
        ]
    )

    return df.filter(
        ~pl.col("anatomy_id").str.contains("∩")
        # NOTE: New versions of the dataset include measurements with an intersection of tissues. We want to remove this measurements.
    ).sort(by=["gene_id", "anatomy_id"])


bgee_node = node(
    run,
    inputs={
        "homo_sapiens_expressions_advanced": "landing.bgee.homo_sapiens_expressions_advanced",
    },
    outputs="bgee.gene_expressions_in_anatomy",
    name="bgee",
    tags=["bronze"],
)
