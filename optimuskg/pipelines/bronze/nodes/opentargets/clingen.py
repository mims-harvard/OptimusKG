from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def process_clingen(
    clingen: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(clingen)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


clingen_node = node(
    process_clingen,
    inputs={"clingen": "landing.opentargets.evidence.clingen"},
    outputs="opentargets.evidence.clingen",
    name="clingen",
    tags=["bronze"],
)
