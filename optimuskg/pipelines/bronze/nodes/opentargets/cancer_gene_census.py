from collections.abc import Callable
from typing import Any

import pandas as pd
from kedro.pipeline import node

from .utils import TargetDiseaseEvidenceSchema, concat_partitions


def run(
    cancer_gene_census: dict[str, Callable[[], Any]],
) -> pd.DataFrame:
    concated_df = concat_partitions(cancer_gene_census)
    df = TargetDiseaseEvidenceSchema.convert(concated_df).df
    return df.to_pandas()  # type: ignore[no-any-return]


cancer_gene_census_node = node(
    run,
    inputs={"cancer_gene_census": "landing.opentargets.evidence.cancer_gene_census"},
    outputs="opentargets.evidence.cancer_gene_census",
    name="cancer_gene_census",
    tags=["bronze"],
)
