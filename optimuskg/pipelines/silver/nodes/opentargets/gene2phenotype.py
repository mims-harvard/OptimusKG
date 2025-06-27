import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import construct_edges


def run(  # noqa: PLR0913
    gene2phenotype: pd.DataFrame,
    phenotypes: pl.DataFrame,
    diseases: pl.DataFrame,
    targets: pl.DataFrame,
    drug_mappings: pl.DataFrame,
    disease_phenotype_ids: pl.DataFrame,
    score_threshold: float,
) -> pl.DataFrame:
    df = pl.from_pandas(gene2phenotype)
    df = (
        df.select(
            [
                "id",
                "targetId",
                "diseaseId",
                "studyId",
                "score",
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


gene2phenotype_node = node(
    run,
    inputs={
        "gene2phenotype": "bronze.opentargets.evidence.gene2phenotype",
        "phenotypes": "bronze.ontology.phenotypes",
        "diseases": "bronze.opentargets.diseases",
        "targets": "bronze.opentargets.targets",
        "disease_phenotype_ids": "bronze.opentargets.disease_phenotype_ids",
        "drug_mappings": "bronze.opentargets.drug_mappings",
        "score_threshold": "params:score_threshold",
    },
    outputs="opentargets.evidence.gene2phenotype",
    name="gene2phenotype",
    tags=["silver"],
)
