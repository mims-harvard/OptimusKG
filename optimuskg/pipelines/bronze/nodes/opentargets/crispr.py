import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions

log = logging.getLogger(__name__)


def process_crispr(
    crispr: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(crispr)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


crispr_node = node(
    process_crispr,
    inputs={"crispr": "landing.opentargets.evidence.crispr"},
    outputs="opentargets.evidence.crispr",
    name="crispr",
)
