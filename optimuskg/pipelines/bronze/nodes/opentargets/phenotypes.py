import logging

import polars as pl
from kedro.pipeline import node

from .utils import KGNodeSchema

log = logging.getLogger(__name__)


def process_phenotypes(
    primekg_nodes: pl.DataFrame,
) -> pl.DataFrame:
    primekg_nodes_df = KGNodeSchema.convert(primekg_nodes).df

    pheno_df = primekg_nodes_df.filter(
        pl.col("node_type") == "effect/phenotype"
    ).clone()
    pheno_df = pheno_df.with_columns(
        [pl.format("HP_{}", pl.col("node_id").str.zfill(7)).alias("id")]
    )

    # Reorder columns with id first
    all_cols = ["id"] + [col for col in pheno_df.columns if col != "id"]
    pheno_df = pheno_df.select(all_cols)

    return pheno_df  # type: ignore[no-any-return]


phenotypes_node = node(
    process_phenotypes,
    inputs={
        "primekg_nodes": "landing.opentargets.primekg_nodes",
    },
    outputs="opentargets.phenotypes",
    name="phenotypes",
)
