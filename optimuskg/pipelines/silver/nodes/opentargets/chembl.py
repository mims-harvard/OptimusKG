import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import construct_edges


def run(  # noqa: PLR0913
    chembl: pd.DataFrame,
    phenotypes: pl.DataFrame,
    diseases: pl.DataFrame,
    targets: pl.DataFrame,
    drug_mappings: pl.DataFrame,
    relation_score_threshold: float,
    display_relation_score: int,
    chembl_score_threshold: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    df = pl.from_pandas(chembl)
    df = (
        df.select(
            [
                "id",
                "drugId",
                "targetId",
                "diseaseId",
                "clinicalPhase",
                "clinicalStatus",
                "score",
                "studyStopReason",
                "studyStartDate",
            ]
        )
        .filter(
            pl.col("drugId").is_in(drug_mappings["id"])
            & pl.col("targetId").is_in(targets["id"])
            & pl.col("diseaseId").is_in(diseases["id"])
            & (pl.col("score") > chembl_score_threshold)
        )
        .with_columns(pl.col("clinicalPhase").cast(pl.Int32))
    )

    # Create drug-disease relationships
    drug_disease = (
        df.select(["score", "drugId", "diseaseId"])
        .group_by(["drugId", "diseaseId"])
        .agg(pl.col("score").mean().alias("score"))
        .with_columns(
            pl.lit("drug_disease").alias("relation"),
            pl.when(pl.col("score") == display_relation_score)
            .then(pl.lit("indication"))
            .when(pl.col("score") < relation_score_threshold)
            .then(pl.lit("weak_clinical_evidence"))
            .otherwise(pl.lit("strong_clinical_evidence"))
            .alias("relation_type"),
        )
        .pipe(
            construct_edges,
            targets_df=pl.DataFrame(targets),
            phenotypes_df=pl.DataFrame(phenotypes),
            diseases_df=diseases,
            drug_mappings_df=drug_mappings,
            type_x="drug",
            type_y="disease",
            relation_label=None,
            display_relation_label=None,
        )
    )

    # Create drug-gene relationships
    drug_gene = (
        df.select(["drugId", "targetId"])
        .unique()
        .with_columns(
            pl.lit("drug_protein").alias("relation"),
            pl.lit("target").alias("relation_type"),
        )
        .pipe(
            construct_edges,
            targets_df=pl.DataFrame(targets),
            phenotypes_df=pl.DataFrame(phenotypes),
            diseases_df=diseases,
            drug_mappings_df=drug_mappings,
            type_x="drug",
            type_y="gene",
            relation_label="drug_protein",
            display_relation_label="target",
        )
    )

    drug_disease = drug_disease.sort(by=sorted(drug_disease.columns))
    drug_gene = drug_gene.sort(by=sorted(drug_gene.columns))

    return drug_disease, drug_gene


chembl_node = node(
    run,
    inputs={
        "chembl": "bronze.opentargets.evidence.chembl",
        "phenotypes": "bronze.ontology.phenotypes",
        "diseases": "bronze.opentargets.diseases",
        "targets": "bronze.opentargets.targets",
        "drug_mappings": "bronze.opentargets.drug_mappings",
        "relation_score_threshold": "params:relation_score_threshold",
        "display_relation_score": "params:display_relation_score",
        "chembl_score_threshold": "params:chembl_score_threshold",
    },
    outputs=[
        "opentargets.evidence.chembl_drug_disease",
        "opentargets.evidence.chembl_drug_gene",
    ],
    name="chembl",
    tags=["silver"],
)
