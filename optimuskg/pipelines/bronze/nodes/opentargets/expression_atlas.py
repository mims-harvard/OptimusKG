from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def run(
    expression_atlas: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(expression_atlas)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


expression_atlas_node = node(
    run,
    inputs={"expression_atlas": "landing.opentargets.evidence.expression_atlas"},
    outputs="opentargets.evidence.expression_atlas",
    name="expression_atlas",
    tags=["bronze"],
)
