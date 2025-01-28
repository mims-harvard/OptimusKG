import logging
from typing import Final

import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import construct_edges

log = logging.getLogger(__name__)


# TODO: This constant should be a parameter in the pipeline.
SCORE_THRESHOLD: Final[float] = 0.5


def process_gene_burden(  # noqa: PLR0913
    gene_burden: pd.DataFrame,
    phenotypes: pl.DataFrame,
    diseases: pl.DataFrame,
    targets: pl.DataFrame,
    drug_mappings: pl.DataFrame,
    disease_phenotype_ids: pl.DataFrame,
) -> pl.DataFrame:
    df = pl.from_pandas(gene_burden)

    df = df.select(
        "id",
        "targetId",
        "diseaseId",
        "score",
        "oddsRatio",
        "pValueExponent",
        "studyCases",
    )
    df = df.filter(pl.col("targetId").is_in(targets["id"]))
    df = df.filter(pl.col("diseaseId").is_in(disease_phenotype_ids["id"]))

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


gene_burden_node = node(
    process_gene_burden,
    inputs={
        "gene_burden": "bronze.opentargets.evidence.gene_burden",
        "phenotypes": "bronze.opentargets.phenotypes",
        "diseases": "bronze.opentargets.diseases",
        "targets": "bronze.opentargets.targets",
        "disease_phenotype_ids": "bronze.opentargets.disease_phenotype_ids",
        "drug_mappings": "bronze.opentargets.drug_mappings",
    },
    outputs="opentargets.evidence.gene_burden",
    name="gene_burden",
)
