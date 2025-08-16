import logging

import polars as pl
from kedro.pipeline import node

from optimuskg.utils import to_snake_case

logger = logging.getLogger(__name__)


def run(
    disease_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    key_cols = ["disease", "phenotype"]
    return (
        disease_phenotype.with_columns(
            pl.struct(
                [c for c in disease_phenotype.columns if c not in key_cols]
            ).alias("metadata")
        )
        .select([*key_cols, "metadata"])
        .rename({col: to_snake_case(col) for col in key_cols})
        .unique(subset=["disease", "phenotype"])
        .sort(by=["disease", "phenotype"])
    )


disease_phenotype_node = node(
    run,
    inputs={
        "disease_phenotype": "landing.opentargets.disease_phenotype",
    },
    outputs="opentargets.disease_phenotype",
    name="disease_phenotype",
    tags=["bronze"],
)
