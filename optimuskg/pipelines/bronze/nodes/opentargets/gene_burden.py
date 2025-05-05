from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def process_gene_burden(
    gene_burden: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(gene_burden)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


gene_burden_node = node(
    process_gene_burden,
    inputs={"gene_burden": "landing.opentargets.evidence.gene_burden"},
    outputs="opentargets.evidence.gene_burden",
    name="gene_burden",
)
