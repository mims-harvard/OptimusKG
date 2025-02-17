import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions

log = logging.getLogger(__name__)


def process_slapenrich(
    slapenrich: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(slapenrich)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


slapenrich_node = node(
    process_slapenrich,
    inputs={"slapenrich": "landing.opentargets.evidence.slapenrich"},
    outputs="opentargets.evidence.slapenrich",
    name="slapenrich",
)
