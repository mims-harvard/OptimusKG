import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    homo_sapiens_expressions_advanced: pl.DataFrame,
    expression_rank_threshold: int,
    call_quality: str,
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

    df = df.filter(
        # TODO: filtering for expression rank/call quality should have a
        # parameter to enable/disable it (to have large/small versions of the final KG)
        # (pl.col("call_quality") == call_quality)
        # & (pl.col("expression_rank") < expression_rank_threshold)
        # & (
        ~pl.col("anatomy_id").str.contains("∩")
        # )  # NOTE: New versions of the dataset include measurements with an intersection of tissues. We want to remove this measurements.
    )

    logger.debug(f"Wrote {len(df)} anatomy-gene pairs")

    return df


bgee_node = node(
    run,
    inputs={
        "homo_sapiens_expressions_advanced": "landing.bgee.homo_sapiens_expressions_advanced",
        "expression_rank_threshold": "params:expression_rank_threshold",
        "call_quality": "params:call_quality",
    },
    outputs="bgee.gene_expressions_in_anatomy",
    name="bgee",
    tags=["bronze"],
)
