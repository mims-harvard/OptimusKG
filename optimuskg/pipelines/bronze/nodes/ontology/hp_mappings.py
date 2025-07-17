import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    hp_mappings: pl.DataFrame,
) -> pl.DataFrame:
    return (
        hp_mappings.filter(
            ~pl.col("database_id").str.contains("DECIPHER:")
        )  # Remove DECIPHER mappings
        .with_columns(
            pl.col("database_id")
            .str.replace_all("OMIM:", "OMIM_", literal=True)
            .str.replace_all("ORPHA:", "Orphanet_", literal=True)
            .alias("database_id"),
            pl.col("hp_id").str.replace_all("HP:", "HP_", literal=True).alias("hp_id"),
        )
        .select(
            pl.col("database_id"),
            pl.col("disease_name"),
            pl.col("hp_id"),
            pl.col("qualifier"),
        )
    )


hp_mappings_node = node(
    run,
    inputs={
        "hp_mappings": "landing.ontology.hp_mappings",
    },
    outputs="ontology.hp_mappings",
    name="hp_mappings",
    tags=["bronze"],
)
