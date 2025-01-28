import logging
from typing import Final

import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import construct_edges

log = logging.getLogger(__name__)


# TODO: This constant should be a parameter in the pipeline.
SCORE_THRESHOLD: Final[float] = 0.5


def process_progeny(  # noqa: PLR0913
    progeny: pd.DataFrame,
    phenotypes: pl.DataFrame,
    diseases: pl.DataFrame,
    targets: pl.DataFrame,
    drug_mappings: pl.DataFrame,
) -> pl.DataFrame:
    df = pl.from_pandas(progeny)

    df = df.select(
        "id",
        "targetId",
        "diseaseId",
        "targetFromSourceId",
        "diseaseFromSource",
        "score",
    )
    df = df.filter(pl.col("targetId").is_in(targets["id"]))
    df = df.filter(pl.col("diseaseId").is_in(diseases["id"]))

    df = construct_edges(
        evidence_df=df,
        targets_df=pl.DataFrame(targets),
        phenotypes_df=pl.DataFrame(phenotypes),
        diseases_df=diseases,
        drug_mappings_df=drug_mappings,
        type_x="gene",
        type_y="disease",
        relation_label="disease_protein",
        display_relation_label="associated with",
    )

    return df


progeny_node = node(
    process_progeny,
    inputs={
        "progeny": "bronze.opentargets.evidence.progeny",
        "phenotypes": "bronze.opentargets.phenotypes",
        "diseases": "bronze.opentargets.diseases",
        "targets": "bronze.opentargets.targets",
        "drug_mappings": "bronze.opentargets.drug_mappings",
    },
    outputs="opentargets.evidence.progeny",
    name="progeny",
)
