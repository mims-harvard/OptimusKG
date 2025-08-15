import logging

import polars as pl
from kedro.pipeline import node

from optimuskg.utils import to_snake_case

logger = logging.getLogger(__name__)


def run(
    disease: pl.DataFrame,
) -> pl.DataFrame:
    key_cols = ["id", "name", "description"]
    return (
        disease.with_columns(
            pl.struct([c for c in disease.columns if c not in key_cols]).alias(
                "metadata"
            )
        )
        .select([*key_cols, "metadata"])
        .rename({col: to_snake_case(col) for col in key_cols})
        .unique(
            subset=["name"]
        )  # TODO: There are diseases from different ontologies that refer to the same disease, like MONDO_0019402 and Orphanet_848.
        .sort(by=["id", "name"])
    )


disease_node = node(
    run,
    inputs={
        "disease": "landing.opentargets.disease",
    },
    outputs="opentargets.disease",
    name="disease",
    tags=["bronze"],
)
