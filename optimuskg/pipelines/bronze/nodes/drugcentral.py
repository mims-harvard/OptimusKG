import logging

import polars as pl
from kedro.pipeline import node

log = logging.getLogger(__name__)


def process_drug_disease(drugcentral: pl.DataFrame) -> pl.DataFrame:
    return drugcentral


drugcentral_node = node(
    process_drug_disease,
    inputs={
        "drugcentral": "landing.drugcentral.psql_dump",
    },
    outputs="drugcentral.drug_disease",
    name="drugcentral",
)
