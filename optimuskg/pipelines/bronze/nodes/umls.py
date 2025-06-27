import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    mrconso: pl.DataFrame,
) -> pl.DataFrame:
    return (
        mrconso.filter(pl.col("language") == "ENG")
        .drop("null_culumn")
        .filter(
            pl.col("abbreviated_source").is_in(
                ["OMIM", "NCI", "MSH", "MDR", "ICD10", "SNOMEDCT_US"]
            )
        )
        .with_columns(
            [
                pl.when(pl.col("abbreviated_source") == "OMIM")
                .then(pl.lit("OMIM:") + pl.col("source_code"))
                .when(pl.col("abbreviated_source") == "NCI")
                .then(pl.lit("NCIT:") + pl.col("source_code"))
                .when(pl.col("abbreviated_source") == "MSH")
                .then(pl.lit("MESH:") + pl.col("source_code"))
                .when(pl.col("abbreviated_source") == "MDR")
                .then(pl.lit("MedDRA:") + pl.col("source_code"))
                .when(pl.col("abbreviated_source") == "ICD10")
                .then(pl.lit("ICD10:") + pl.col("source_code"))
                .when(pl.col("abbreviated_source") == "SNOMEDCT_US")
                .then(pl.lit("SCTID:") + pl.col("source_code"))
                .otherwise(pl.col("source_code"))
                .alias("source_code")
            ]
        )
        .select(
            [
                "cui",
                "source_cui",
                "source_descriptor_dui",
                "abbreviated_source",
                "source_code",
            ]
        )
        .unique()
    )


umls_node = node(
    run,
    inputs={"mrconso": "landing.umls.mrconso"},
    outputs="umls.mrconso",
    name="umls",
    tags=["bronze"],
)
