from typing import final
import logging

import polars as pl
from typedframe import PolarsTypedFrame
from kedro.pipeline import node

log = logging.getLogger(__name__)

from typing import Final


@final
class ReactomeNcbi(PolarsTypedFrame):
    schema: Final = {
        "ncbi_id": pl.Utf8,
        "reactome_id": pl.String,
        "url": pl.String,
        "reactome_name": pl.String,
        "evidence_code": pl.String,
        "species": pl.String,
    }


def process_reactome(
    ncbi2_reactome: pl.DataFrame,
) -> ReactomeNcbi:
    df_ncbi = ncbi2_reactome.filter(pl.col("species") == "Homo sapiens")
    df_ncbi = df_ncbi.drop(["species"])
    df_ncbi = df_ncbi.unique()
    return ReactomeNcbi.convert(df_ncbi)


reactome_ncbi_node = node(
    process_reactome,
    inputs=dict(ncbi2_reactome="landing.reactome.ncbi2_reactome"),
    outputs="reactome.reactome_ncbi",
    name="reactome_ncbi",
)
