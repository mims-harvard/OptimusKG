import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions

log = logging.getLogger(__name__)


def process_reactome(
    reactome: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(reactome)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


reactome_node = node(
    process_reactome,
    inputs={"reactome": "landing.opentargets.evidence.reactome"},
    outputs="opentargets.evidence.reactome",
    name="reactome",
)
