import logging

import polars as pl
from kedro.pipeline import node

from optimuskg.utils import to_snake_case

logger = logging.getLogger(__name__)


def run(
    drug_molecule: pl.DataFrame,
) -> pl.DataFrame:
    key_cols = ["id", "name"]
    return (
        drug_molecule.with_columns(
            pl.struct([c for c in drug_molecule.columns if c not in key_cols]).alias(
                "metadata"
            )
        )
        .select([*key_cols, "metadata"])
        .rename({col: to_snake_case(col) for col in key_cols})
        .unique(subset=["id", "name"])
        .sort(by=["id", "name"])
    )


drug_molecule_node = node(
    run,
    inputs={
        "drug_molecule": "landing.opentargets.drug_molecule",
    },
    outputs="opentargets.drug_molecule",
    name="drug_molecule",
    tags=["bronze"],
)
