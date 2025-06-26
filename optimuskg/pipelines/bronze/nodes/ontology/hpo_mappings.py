import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def process_hpo_mappings(
    hpo_mappings: pl.DataFrame,
) -> pl.DataFrame:
    return (
        hpo_mappings.filter(
            ~pl.col("database_id").str.contains("DECIPHER:")
        )  # Remove DECIPHER mappings
        .with_columns(
            pl.col("database_id")
            .str.replace_all("OMIM:", "OMIM_", literal=True)
            .str.replace_all("ORPHA:", "Orphanet_", literal=True)
            .alias("database_id"),
            pl.col("hpo_id")
            .str.replace_all("HP:", "HP_", literal=True)
            .alias("hpo_id"),
        )
        .select(
            pl.col("database_id"),
            pl.col("disease_name"),
            pl.col("hpo_id"),
            pl.col("qualifier"),
        )
    )


hpo_mappings_node = node(
    process_hpo_mappings,
    inputs={
        "hpo_mappings": "landing.ontology.hpo_mappings",
    },
    outputs="ontology.hpo_mappings",
    name="hpo_mappings",
    tags=["bronze"],
)
