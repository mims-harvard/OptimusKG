import logging

import polars as pl
from kedro.pipeline import node

log = logging.getLogger(__name__)


def process_reactome_ncbi(
    ncbi2_reactome_df: pl.DataFrame,
) -> pl.DataFrame:
    df = ncbi2_reactome_df.filter(pl.col("species") == "Homo sapiens")
    df = df.drop(["species"])
    df = df.unique()

    return df


reactome_ncbi_node = node(
    process_reactome_ncbi,
    inputs={"ncbi2_reactome_df": "landing.reactome.ncbi2_reactome"},
    outputs="reactome.reactome_ncbi",
    name="reactome_ncbi",
)
