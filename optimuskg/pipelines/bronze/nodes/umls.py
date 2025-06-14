import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def process_umls(
    mrconso: pl.DataFrame,
) -> pl.DataFrame:
    return mrconso.filter(pl.col("language") == "ENG").drop("null_culumn")


umls_node = node(
    process_umls,
    inputs={"mrconso": "landing.umls.mrconso"},
    outputs="umls.mrconso",
    name="umls",
    tags=["bronze"],
)
