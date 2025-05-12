from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def process_intogen(
    intogen: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(intogen)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


intogen_node = node(
    process_intogen,
    inputs={"intogen": "landing.opentargets.evidence.intogen"},
    outputs="opentargets.evidence.intogen",
    name="intogen",
    tags=["bronze"],
)
