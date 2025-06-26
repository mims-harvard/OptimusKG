import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import construct_edges


def process_genomics_england(  # noqa: PLR0913
    genomics_england: pd.DataFrame,
    phenotypes: pl.DataFrame,
    diseases: pl.DataFrame,
    targets: pl.DataFrame,
    drug_mappings: pl.DataFrame,
    disease_phenotype_ids: pl.DataFrame,
    score_threshold: float,
) -> pl.DataFrame:
    df = pl.from_pandas(genomics_england)
    df = (
        df.select(
            [
                "id",
                "targetId",
                "diseaseId",
                "score",
                "studyId",
                "studyOverview",
            ]
        )
        .filter(pl.col("targetId").is_in(targets["id"]))
        .filter(pl.col("diseaseId").is_in(disease_phenotype_ids["id"]))
        .filter(pl.col("score") > score_threshold)
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


genomics_england_node = node(
    process_genomics_england,
    inputs={
        "genomics_england": "bronze.opentargets.evidence.genomics_england",
        "phenotypes": "bronze.ontology.phenotypes",
        "diseases": "bronze.opentargets.diseases",
        "targets": "bronze.opentargets.targets",
        "disease_phenotype_ids": "bronze.opentargets.disease_phenotype_ids",
        "drug_mappings": "bronze.opentargets.drug_mappings",
        "score_threshold": "params:score_threshold",
    },
    outputs="opentargets.evidence.genomics_england",
    name="genomics_england",
    tags=["silver"],
)
