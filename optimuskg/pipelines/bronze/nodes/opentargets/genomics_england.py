from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def process_genomics_england(
    genomics_england: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(genomics_england)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


genomics_england_node = node(
    process_genomics_england,
    inputs={"genomics_england": "landing.opentargets.evidence.genomics_england"},
    outputs="opentargets.evidence.genomics_england",
    name="genomics_england",
    tags=["bronze"],
)
