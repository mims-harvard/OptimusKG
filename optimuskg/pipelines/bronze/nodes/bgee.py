import logging
from typing import Final

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)

# TODO: This constant should be a parameter in the pipeline.
EXPRESSION_RANK_THRESHOLD: Final[int] = 25000


def process_bgee(
    homo_sapiens_expressions_advanced: pl.DataFrame,
) -> pl.DataFrame:
    # Filter rows where 'Anatomical entity ID' starts with 'UBERON'
    df = homo_sapiens_expressions_advanced.filter(
        pl.col("Anatomical entity ID").str.starts_with("UBERON")
    )

    # Select and rename relevant columns
    df = df.select(
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

    df = df.filter(
        (
            pl.col("call_quality") == "gold quality"
        )  # We only take the highest quality datapoints
        & (
            pl.col("expression_rank") < EXPRESSION_RANK_THRESHOLD
        )  # We take the most expressing genes within each tissue
        & (
            ~pl.col("anatomy_id").str.contains("∩")
        )  # NOTE: New versions of the dataset include measurements with an intersection of tissues. We want to remove this measurements.
    )

    logger.debug(f"Wrote {len(df)} anatomy-gene pairs")

    return df


bgee_node = node(
    process_bgee,
    inputs={
        "homo_sapiens_expressions_advanced": "landing.bgee.homo_sapiens_expressions_advanced"
    },
    outputs="bgee.gene_expressions_in_anatomy",
    name="bgee",
)
