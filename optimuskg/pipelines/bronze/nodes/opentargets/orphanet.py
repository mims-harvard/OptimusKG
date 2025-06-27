from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def run(
    orphanet: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(orphanet)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


orphanet_node = node(
    run,
    inputs={"orphanet": "landing.opentargets.evidence.orphanet"},
    outputs="opentargets.evidence.orphanet",
    name="orphanet",
    tags=["bronze"],
)
