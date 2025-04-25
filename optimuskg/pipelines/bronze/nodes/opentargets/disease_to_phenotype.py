import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import concat_json_partitions

log = logging.getLogger(__name__)


def process_disease_to_phenotype(
    disease_to_phenotype: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_json_partitions(disease_to_phenotype)
    df = concated_df.sort(by=["disease", "phenotype"])
    return df.to_pandas()


disease_to_phenotype_node = node(
    process_disease_to_phenotype,
    inputs={
        "disease_to_phenotype": "landing.opentargets.disease_to_phenotype",
    },
    outputs="opentargets.disease_to_phenotype",
    name="disease_to_phenotype",
)
