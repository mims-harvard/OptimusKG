from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def run(
    chembl: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(chembl)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


chembl_node = node(
    run,
    inputs={"chembl": "landing.opentargets.evidence.chembl"},
    outputs="opentargets.evidence.chembl",
    name="chembl",
    tags=["bronze"],
)
