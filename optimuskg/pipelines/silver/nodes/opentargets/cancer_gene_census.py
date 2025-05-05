from typing import Final

import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import construct_edges

# TODO: This constant should be a parameter in the pipeline.
SCORE_THRESHOLD: Final[float] = 0.5


def process_cancer_gene_census(  # noqa: PLR0913
    cancer_gene_census: pd.DataFrame,
    phenotypes: pl.DataFrame,
    diseases: pl.DataFrame,
    targets: pl.DataFrame,
    drug_mappings: pl.DataFrame,
    disease_phenotype_ids: pl.DataFrame,
) -> pl.DataFrame:
    df = pl.from_pandas(cancer_gene_census)
    df = (
        df.select(["id", "targetId", "diseaseId", "studyId", "score"])
        .filter(
            (pl.col("targetId").cast(str).is_in(targets["id"]))
            & (pl.col("diseaseId").cast(str).is_in(disease_phenotype_ids["id"]))
            & (pl.col("score") > SCORE_THRESHOLD)
        )
        .pipe(
            construct_edges,
            targets_df=pl.DataFrame(targets),
            phenotypes_df=pl.DataFrame(phenotypes),
            diseases_df=diseases,
            drug_mappings_df=drug_mappings,
            type_x="gene",
            type_y="disease",
            relation_label="disease_protein",
            display_relation_label="associated with",
        )
    )
    df = df.sort(by=sorted(df.columns))
    return df


cancer_gene_census_node = node(
    process_cancer_gene_census,
    inputs={
        "cancer_gene_census": "bronze.opentargets.evidence.cancer_gene_census",
        "phenotypes": "bronze.opentargets.phenotypes",
        "diseases": "bronze.opentargets.diseases",
        "targets": "bronze.opentargets.targets",
        "disease_phenotype_ids": "bronze.opentargets.disease_phenotype_ids",
        "drug_mappings": "bronze.opentargets.drug_mappings",
    },
    outputs="opentargets.evidence.cancer_gene_census",
    name="cancer_gene_census",
)
