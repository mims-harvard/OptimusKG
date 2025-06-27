from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def run(
    crispr_screen: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(crispr_screen)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


crispr_screen_node = node(
    run,
    inputs={"crispr_screen": "landing.opentargets.evidence.crispr_screen"},
    outputs="opentargets.evidence.crispr_screen",
    name="crispr_screen",
    tags=["bronze"],
)
