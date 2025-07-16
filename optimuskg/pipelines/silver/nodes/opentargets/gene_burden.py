import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import construct_edges


def run(  # noqa: PLR0913
    gene_burden: pd.DataFrame,
    hpo_terms: pl.DataFrame,
    diseases: pl.DataFrame,
    targets: pl.DataFrame,
    drug_mappings: pl.DataFrame,
    disease_phenotype_ids: pl.DataFrame,
) -> pl.DataFrame:
    df = pl.from_pandas(gene_burden)
    df = (
        df.select(
            [
                "id",
                "targetId",
                "diseaseId",
                "score",
                "oddsRatio",
                "pValueExponent",
                "studyCases",
            ]
        )
        .filter(pl.col("targetId").is_in(targets["id"]))
        .filter(pl.col("diseaseId").is_in(disease_phenotype_ids["id"]))
        .pipe(
            construct_edges,
            targets_df=pl.DataFrame(targets),
            phenotypes_df=pl.DataFrame(hpo_terms),
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


gene_burden_node = node(
    run,
    inputs={
        "gene_burden": "bronze.opentargets.evidence.gene_burden",
        "hpo_terms": "bronze.ontology.hpo_terms",
        "diseases": "bronze.opentargets.diseases",
        "targets": "bronze.opentargets.targets",
        "disease_phenotype_ids": "bronze.opentargets.disease_phenotype_ids",
        "drug_mappings": "bronze.opentargets.drug_mappings",
    },
    outputs="opentargets.evidence.gene_burden",
    name="gene_burden",
    tags=["silver"],
)
