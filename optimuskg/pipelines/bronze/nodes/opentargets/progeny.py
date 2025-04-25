import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions

log = logging.getLogger(__name__)


def process_progeny(
    progeny: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(progeny)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    df = df.sort("id")
    return df.to_pandas()  # type: ignore[no-any-return]


progeny_node = node(
    process_progeny,
    inputs={"progeny": "landing.opentargets.evidence.progeny"},
    outputs="opentargets.evidence.progeny",
    name="progeny",
)
