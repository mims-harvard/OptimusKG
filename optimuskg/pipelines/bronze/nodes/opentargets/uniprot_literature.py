from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def process_uniprot_literature(
    uniprot_literature: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(uniprot_literature)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


uniprot_literature_node = node(
    process_uniprot_literature,
    inputs={"uniprot_literature": "landing.opentargets.evidence.uniprot_literature"},
    outputs="opentargets.evidence.uniprot_literature",
    name="uniprot_literature",
)
