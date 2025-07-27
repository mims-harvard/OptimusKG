import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)

CLINICAL_PHASE_MAPPING = (
    pl.when(pl.col("clinical_phase") == 4)  # noqa: PLR2004
    .then(pl.lit("Phase IV"))
    .when(pl.col("clinical_phase") == 3)  # noqa: PLR2004
    .then(pl.lit("Phase III"))
    .when(pl.col("clinical_phase") == 2)  # noqa: PLR2004
    .then(pl.lit("Phase II"))
    .when(pl.col("clinical_phase") == 1)  # noqa: PLR2004
    .then(pl.lit("Phase I"))
    .when(pl.col("clinical_phase") == 0.5)  # noqa: PLR2004
    .then(pl.lit("Early Phase I"))
    .when(pl.col("clinical_phase").is_null())
    .then(pl.lit("Preclinical"))
    .otherwise(pl.lit("Unknown"))
)


def run(  # noqa: PLR0913
    # opentargets
    evidence: pl.DataFrame,
    targets: pl.DataFrame,
    diseases: pl.DataFrame,
    drug_mappings: pl.DataFrame,
    score_threshold: float,
    display_relation_score: float,
    # drugbank
    drug_disease: pl.DataFrame,
    umls_mondo: pl.DataFrame,
    mondo_terms: pl.DataFrame,
    vocabulary: pl.DataFrame,
) -> pl.DataFrame:
    opentargets_drug_disease = (
        evidence.filter(
            pl.col("target_id").is_in(targets["id"]),
        )
        .group_by(["drug_id", "disease_id"])
        .agg(
            pl.col("score").mean().alias("mean_score"),
            pl.when(pl.col("study_id").implode().list.len() > 0)
            .then(pl.col("study_id").implode().list.join("|"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("study_ids"),
            pl.when(pl.col("study_start_date").implode().list.len() > 0)
            .then(pl.col("study_start_date").implode().list.join("|"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("study_start_dates"),
            pl.when(pl.col("clinical_status").implode().list.len() > 0)
            .then(pl.col("clinical_status").implode().list.join("|"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("clinical_statuses"),
            pl.when(CLINICAL_PHASE_MAPPING.implode().list.len() > 0)
            .then(CLINICAL_PHASE_MAPPING.implode().list.join("|"))
            .otherwise(pl.lit(None, dtype=pl.Int8))
            .alias("clinical_phases"),
        )
        .join(
            drug_mappings.select(["id", "name"]),
            left_on="drug_id",
            right_on="id",
            how="inner",
        )
        .rename({"name": "drug_name"})
        .join(
            diseases.select(["id", "name"]),
            left_on="disease_id",
            right_on="id",
            how="inner",
        )
        .rename({"name": "disease_name"})
        .with_columns(
            pl.lit("drug_disease").alias("relation"),
            pl.when(pl.col("mean_score") == display_relation_score)
            .then(pl.lit("indication"))
            .when(pl.col("mean_score") < score_threshold)
            .then(pl.lit("weak_clinical_evidence"))
            .otherwise(pl.lit("strong_clinical_evidence"))
            .alias("relation_type"),
            pl.col("drug_id").alias("x_id"),
            pl.lit("drug").alias("x_type"),
            pl.col("drug_name").alias("x_name"),
            pl.lit("opentargets").alias("x_source"),
            pl.col("clinical_statuses").alias("clinical_statuses"),
            pl.col("disease_id").alias("y_id"),
            pl.lit("disease").alias("y_type"),
            pl.col("disease_name").alias("y_name"),
            pl.lit("opentargets").alias("y_source"),
        )
        .unique()
        .select(
            [
                "relation",
                "relation_type",
                "x_id",
                "x_type",
                "x_name",
                "x_source",
                "y_id",
                "y_type",
                "y_name",
                "y_source",
                # NOTE: metadata
                "mean_score",
                "study_ids",
                "study_start_dates",
                "clinical_statuses",
                "clinical_phases",
            ]
        )
    )

    drugbank_drug_disease = (
        drug_disease.join(vocabulary, left_on="cas_reg_no", right_on="cas", how="inner")
        .join(umls_mondo, left_on="umls_cui", right_on="umls_id", how="inner")
        .join(mondo_terms, left_on="mondo_id", right_on="id", how="inner")
        .select(["relationship_name", "drugbank_id", "common_name", "mondo_id", "name"])
        .rename(
            {
                "drugbank_id": "x_id",
                "mondo_id": "y_id",
                "common_name": "x_name",
                "name": "y_name",
                "relationship_name": "relation_type",
            }
        )
        .with_columns(
            [
                pl.lit("drug").alias("x_type"),
                pl.lit("DrugBank").alias("x_source"),
                pl.lit("disease").alias("y_type"),
                pl.lit("MONDO").alias("y_source"),
                pl.lit("drug_disease").alias("relation"),
            ]
        )
        .select(
            [
                "relation",
                "relation_type",
                "x_id",
                "x_type",
                "x_name",
                "x_source",
                "y_id",
                "y_type",
                "y_name",
                "y_source",
            ]
        )
        .unique(subset=["x_name", "y_name"])
    )

    return (
        pl.concat([opentargets_drug_disease, drugbank_drug_disease], how="diagonal")
        .unique(subset=["x_name", "y_name"])
        .sort(by=["x_id", "y_id"])
    )


drug_disease_node = node(
    run,
    inputs={
        # opentargets
        "evidence": "bronze.opentargets.evidence",
        "targets": "bronze.opentargets.targets",
        "diseases": "bronze.opentargets.diseases",
        "drug_mappings": "bronze.opentargets.drug_mappings",
        "score_threshold": "params:score_threshold",
        "display_relation_score": "params:display_relation_score",
        # drugbank
        "drug_disease": "bronze.drug_disease",
        "umls_mondo": "silver.umls.umls_mondo",
        "mondo_terms": "bronze.ontology.mondo_terms",
        "vocabulary": "bronze.drugbank.vocabulary",
    },
    outputs="drug_disease",
    name="drug_disease",
    tags=["silver"],
)
