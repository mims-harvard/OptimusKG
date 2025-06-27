from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def run(
    crispr: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(crispr)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


crispr_node = node(
    run,
    inputs={"crispr": "landing.opentargets.evidence.crispr"},
    outputs="opentargets.evidence.crispr",
    name="crispr",
    tags=["bronze"],
)
