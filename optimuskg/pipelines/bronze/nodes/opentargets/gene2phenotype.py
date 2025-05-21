from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def process_gene2phenotype(
    gene2phenotype: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(gene2phenotype)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


gene2phenotype_node = node(
    process_gene2phenotype,
    inputs={"gene2phenotype": "landing.opentargets.evidence.gene2phenotype"},
    outputs="opentargets.evidence.gene2phenotype",
    name="gene2phenotype",
    tags=["bronze"],
)
