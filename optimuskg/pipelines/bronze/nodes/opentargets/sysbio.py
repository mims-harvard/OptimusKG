from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def process_sysbio(
    sysbio: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(sysbio)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


sysbio_node = node(
    process_sysbio,
    inputs={"sysbio": "landing.opentargets.evidence.sysbio"},
    outputs="opentargets.evidence.sysbio",
    name="sysbio",
)
