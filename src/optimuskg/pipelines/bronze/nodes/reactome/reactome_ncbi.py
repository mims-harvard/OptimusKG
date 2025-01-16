import logging
from typing import Final, final

import polars as pl
from kedro.pipeline import node
from typedframe import PolarsTypedFrame

log = logging.getLogger(__name__)


@final
class LandingReactomeNcbi(PolarsTypedFrame):
    schema: Final = {
        "ncbi_id": pl.Utf8,
        "reactome_id": pl.String,
        "url": pl.String,
        "reactome_name": pl.String,
        "evidence_code": pl.String,
        "species": pl.String,
    }


@final
class BronzeReactomeNcbi(PolarsTypedFrame):
    schema: Final = {
        "ncbi_id": pl.Utf8,
        "reactome_id": pl.String,
        "url": pl.String,
        "reactome_name": pl.String,
        "evidence_code": pl.String,
    }


def process_reactome_ncbi(
    ncbi2_reactome: pl.DataFrame,
) -> pl.DataFrame:
    df_ncbi = LandingReactomeNcbi.convert(ncbi2_reactome).df
    df_ncbi = df_ncbi.filter(pl.col("species") == "Homo sapiens")
    df_ncbi = df_ncbi.drop(["species"])
    df_ncbi = df_ncbi.unique()
    return BronzeReactomeNcbi.convert(df_ncbi).df


reactome_ncbi_node = node(
    process_reactome_ncbi,
    inputs={"ncbi2_reactome": "landing.reactome.ncbi2_reactome"},
    outputs="reactome.reactome_ncbi",
    name="reactome_ncbi",
)
