import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions

log = logging.getLogger(__name__)


def process_chembl(
    chembl: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(chembl)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    df = df.sort("id")
    return df.to_pandas()  # type: ignore[no-any-return]


chembl_node = node(
    process_chembl,
    inputs={"chembl": "landing.opentargets.evidence.chembl"},
    outputs="opentargets.evidence.chembl",
    name="chembl",
)
