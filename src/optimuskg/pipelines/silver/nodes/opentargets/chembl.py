import logging
from typing import Final

import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import construct_edges

log = logging.getLogger(__name__)


# TODO: This constant should be a parameter in the pipeline.
RELATION_SCORE_THRESHOLD: Final[float] = 0.5
DISPLAY_RELATION_SCORE: Final[int] = 1
CHEMBL_SCORE_THRESHOLD: Final[float] = 0.05


def process_chembl(  # noqa: PLR0913
    chembl: pd.DataFrame,
    phenotypes: pl.DataFrame,
    diseases: pl.DataFrame,
    targets: pl.DataFrame,
    drug_mappings: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    df = pl.from_pandas(chembl)

    df = df.select(
        "id",
        "drugId",
        "targetId",
        "diseaseId",
        "clinicalPhase",
        "clinicalStatus",
        "score",
        "studyStopReason",
        "studyStartDate",
    )

    df = (
        df.filter(pl.col("drugId").is_in(drug_mappings["id"]))
        .filter(pl.col("targetId").is_in(targets["id"]))
        .filter(pl.col("diseaseId").is_in(diseases["id"]))
        .filter(pl.col("score") > CHEMBL_SCORE_THRESHOLD)
    )

    df = df.with_columns(pl.col("clinicalPhase").cast(pl.Int32))

    # Create drug-disease relationships
    drug_disease = (
        df.select(["score", "drugId", "diseaseId"])
        .group_by(["drugId", "diseaseId"])
        .agg(pl.col("score").mean().alias("score"))
    )

    # Add relation columns using when-then-otherwise logic
    drug_disease = drug_disease.with_columns(
        [
            pl.when(pl.col("score") == 1)
            .then(pl.lit("indication"))
            .when(pl.col("score") < RELATION_SCORE_THRESHOLD)
            .then(pl.lit("weak_clinical_evidence"))
            .otherwise(pl.lit("strong_clinical_evidence"))
            .alias("relation"),
            pl.when(pl.col("score") == DISPLAY_RELATION_SCORE)
            .then(pl.lit("indication"))
            .otherwise(pl.lit("clinical candidate"))
            .alias("display_relation"),
        ]
    )

    # Create drug-gene relationships
    drug_gene = (
        df.select(["drugId", "targetId"])
        .unique()
        .with_columns(
            [
                pl.lit("drug_protein").alias("relation"),
                pl.lit("target").alias("display_relation"),
            ]
        )
    )

    drug_disease = construct_edges(
        evidence_df=drug_disease,
        targets_df=pl.DataFrame(targets),
        phenotypes_df=pl.DataFrame(phenotypes),
        diseases_df=diseases,
        drug_mappings_df=drug_mappings,
        type_x="drug",
        type_y="disease",
        relation_label=None,
        display_relation_label=None,
    )

    drug_gene = construct_edges(
        evidence_df=drug_gene,
        targets_df=pl.DataFrame(targets),
        phenotypes_df=pl.DataFrame(phenotypes),
        diseases_df=diseases,
        drug_mappings_df=drug_mappings,
        type_x="drug",
        type_y="gene",
        relation_label="drug_protein",
        display_relation_label="target",
    )

    return drug_disease, drug_gene


chembl_node = node(
    process_chembl,
    inputs={
        "chembl": "bronze.opentargets.evidence.chembl",
        "phenotypes": "bronze.opentargets.phenotypes",
        "diseases": "bronze.opentargets.diseases",
        "targets": "bronze.opentargets.targets",
        "drug_mappings": "bronze.opentargets.drug_mappings",
    },
    outputs=[
        "opentargets.evidence.chembl_drug_disease",
        "opentargets.evidence.chembl_drug_gene",
    ],
    name="chembl",
)
