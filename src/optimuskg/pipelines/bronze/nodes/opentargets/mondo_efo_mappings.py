import logging
from typing import Final, final

import polars as pl
from kedro.pipeline import node
from typedframe import PolarsTypedFrame

log = logging.getLogger(__name__)


@final
class MondoEfoMappingsSchema(PolarsTypedFrame):
    schema: Final[dict[str, type[pl.DataType] | pl.List]] = {
        "mondo_id": pl.String,
        "efo_id": pl.String,
        "mondo_name": pl.String,
        "efo_name": pl.String,
    }


def process_mondo_efo_mappings(
    mondo_efo_mappings: pl.DataFrame,
) -> pl.DataFrame:
    df = MondoEfoMappingsSchema.convert(mondo_efo_mappings).df

    df = df.with_columns(
        [
            pl.col("mondo_id").str.replace("http://purl.obolibrary.org/obo/", ""),
            pl.col("efo_id")
            .str.replace("http://www.ebi.ac.uk/efo/", "")
            .str.replace("http://www.orpha.net/ORDO/", "")
            .str.replace("http://purl.obolibrary.org/obo/", ""),
        ]
    )

    df = df.select(["mondo_id", "efo_id"])
    return df  # type: ignore[no-any-return]


mondo_efo_mappings_node = node(
    process_mondo_efo_mappings,
    inputs={"mondo_efo_mappings": "landing.opentargets.mondo_efo_mappings"},
    outputs="opentargets.mondo_efo_mappings",
    name="mondo_efo_mappings",
)
