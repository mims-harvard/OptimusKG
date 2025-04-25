import logging

import polars as pl
from kedro.pipeline import node

log = logging.getLogger(__name__)


def process_phenotypes(
    primekg_nodes_df: pl.DataFrame,
) -> pl.DataFrame:
    pheno_df = primekg_nodes_df.filter(
        pl.col("node_type") == "effect/phenotype"
    ).clone()
    pheno_df = pheno_df.with_columns(
        [pl.format("HP_{}", pl.col("node_id").str.zfill(7)).alias("id")]
    )

    # Reorder columns with id first
    all_cols = ["id"] + [col for col in pheno_df.columns if col != "id"]
    df = pheno_df.select(all_cols)
    df = df.sort(by=sorted(df.columns))
    return df


phenotypes_node = node(
    process_phenotypes,
    inputs={
        "primekg_nodes_df": "landing.opentargets.primekg_nodes",
    },
    outputs="opentargets.phenotypes",
    name="phenotypes",
)
