from typing import Any, final
import logging

import polars as pl
from typedframe import PolarsTypedFrame
from kedro.pipeline import node

log = logging.getLogger(__name__)


@final
class HomoSapiensExpressionsAdvanced(PolarsTypedFrame):
    """Raw data schema for the landing zone. Some columns have extra spaces"""

    schema = {
        "Gene ID": pl.String,
        "Gene name": pl.String,
        "Anatomical entity ID": pl.String,
        "Anatomical entity name": pl.String,
        "Expression": pl.String,
        "Call quality": pl.String,
        "Expression rank": pl.Int64,
        "Including observed data": pl.Utf8,
        "Affymetrix data": pl.Utf8,
        "Affymetrix  experiment count showing expression of this gene in this condition or in sub-conditions with a high quality": pl.Int64,
        "Affymetrix experiment count showing expression of this gene in this condition or in sub-conditions with a low quality": pl.Int64,
        "Affymetrix experiment count showing absence of expression of this gene in this condition or valid parent conditions with a high quality": pl.Int64,
        "Affymetrix experiment count showing absence of expression of this gene in this condition or valid parent conditions with a low quality": pl.Int64,
        "Including Affymetrix observed data": pl.Utf8,
        "EST data": pl.Utf8,
        "EST experiment count showing expression of this gene in this condition or in sub-conditions with a high quality": pl.Int64,
        "EST experiment count showing expression of this gene in this condition or in sub-conditions with a low quality": pl.Int64,
        "Including EST observed data": pl.Utf8,
        "In situ hybridization data": pl.Utf8,
        "In situ hybridization experiment count showing expression of this gene in this condition or in sub-conditions with a high quality": pl.Int64,
        "In situ hybridization experiment count showing expression of this gene in this condition or in sub-conditions with a low quality": pl.Int64,
        "In situ hybridization experiment count showing absence of expression of this gene in this condition or valid parent conditions with a high quality": pl.Int64,
        "In situ hybridization experiment count showing absence of expression of this gene in this condition or valid parent conditions with a low quality": pl.Int64,
        "Including in situ hybridization observed data": pl.Utf8,
        "RNA-Seq data": pl.Utf8,
        "RNA-Seq  experiment count showing expression of this gene in this condition or in sub-conditions with a high quality": pl.Int64,
        "RNA-Seq  experiment count showing expression of this gene in this condition or in sub-conditions with a low quality": pl.Int64,
        "RNA-Seq  experiment count showing absence of expression of this gene in this condition or valid parent conditions with a high quality": pl.Int64,
        "RNA-Seq  experiment count showing absence of expression of this gene in this condition or valid parent conditions with a low quality": pl.Int64,
        "Including RNA-Seq observed data": pl.Utf8,
    }


@final
class GeneExpressionsInAnatomy(PolarsTypedFrame):
    schema = {
        "gene_id": pl.String,
        "gene_name": pl.String,
        "anatomy_id": pl.String,
        "anatomy_name": pl.String,
        "expression": pl.String,
        "call_quality": pl.String,
        "expression_rank": pl.String,
    }


def process_bgee(
    homo_sapiens_expressions_advanced: pl.DataFrame,
) -> pl.DataFrame:
    df = HomoSapiensExpressionsAdvanced.convert(homo_sapiens_expressions_advanced).df
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
            pl.col("expression_rank")
            < 25000  # TODO: This magic number should be a parameter in the pipeline.
        )  # We take the most expressing genes within each tissue
        & (
            ~pl.col("anatomy_id").str.contains("∩")
        )  # NOTE: New versions of the dataset include measurements with an intersection of tissues. We want to remove this measurements.
    )

    log.info(f"Wrote {len(df)} anatomy-gene pairs")
    return GeneExpressionsInAnatomy.convert(df).df


bgee_node = node(
    process_bgee,
    inputs=dict(
        homo_sapiens_expressions_advanced="landing.bgee.homo_sapiens_expressions_advanced"
    ),
    outputs="bgee.gene_expressions_in_anatomy",
    name="bgee",
)
