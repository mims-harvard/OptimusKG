import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions

log = logging.getLogger(__name__)


def process_crispr_screen(
    crispr_screen: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(crispr_screen)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


crispr_screen_node = node(
    process_crispr_screen,
    inputs={"crispr_screen": "landing.opentargets.evidence.crispr_screen"},
    outputs="opentargers.evidence.crispr_screen",
    name="crispr_screen",
)
