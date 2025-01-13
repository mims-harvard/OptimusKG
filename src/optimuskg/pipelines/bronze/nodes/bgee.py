from typing import Any, final
import logging

import polars as pl
from typedframe import PolarsTypedFrame, TypedDataFrame
from kedro.pipeline import node

log = logging.getLogger(__name__)


@final
class HomoSapiensExpressionsAdvanced(PolarsTypedFrame):
    schema = {
        "Gene ID": str,
        "Gene name": str,
        "Anatomical entity ID": str,
        "Anatomical entity name": str,
        "Expression": str,
        "Call quality": str,
        "Expression rank": int,
    }


@final
class GeneExpressionsInAnatomy(PolarsTypedFrame):
    schema = {
        "gene_id": str,
        "gene_name": str,
        "anatomy_id": str,
        "anatomy_name": str,
        "expression": str,
        "call_quality": str,
        "expression_rank": str,
    }


def process_bgee(
    homo_sapiens_expressions_advanced: pl.DataFrame,
) -> GeneExpressionsInAnatomy:
    df = HomoSapiensExpressionsAdvanced.convert(homo_sapiens_expressions_advanced)
    # Filter rows where 'Anatomical entity ID' starts with 'UBERON'
    df = df.filter(pl.col("Anatomical entity ID").str.starts_with("UBERON"))

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
            pl.col("expression_rank") < 25000
        )  # We take the most expressing genes within each tissue
        & (
            ~pl.col("anatomy_id").str.contains("∩")
        )  # NOTE: New versions of the dataset include measurements with an intersection of tissues. We want to remove this measurements.
    )

    log.info(f"Wrote {len(df)} anatomy-gene pairs")
    return GeneExpressionsInAnatomy.convert(df)


bgee_node = node(
    process_bgee,
    {
        "homo_sapiens_expressions_advanced": "landing.bgee.homo_sapiens_expressions_advanced"
    },
    "gene_expressions_in_anatomy",
    name="bgee",
)
