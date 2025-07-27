import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(  # noqa: PLR0913
    # opentargets
    evidence: pl.DataFrame,
    targets: pl.DataFrame,
    diseases: pl.DataFrame,
    disease_phenotype_ids: pl.DataFrame,
    score_threshold: float,
    # disgenet
    disgenet_diseases: pl.DataFrame,
    umls_mondo: pl.DataFrame,
    mondo_terms: pl.DataFrame,
) -> pl.DataFrame:
    opentargets_disease_protein = (
        evidence.filter(
            ~pl.col("disease_id").str.contains("HP"),
            pl.col("target_id").cast(str).is_in(targets["id"]),
            pl.col("disease_id").cast(str).is_in(disease_phenotype_ids["id"]),
            pl.col("score") > score_threshold,
        )
        .with_columns(
            pl.lit("disease_protein").alias("relation"),
            pl.when(pl.col("log2_fold_change_value") < 0)
            .then(pl.lit("expression_downregulated"))
            .when(pl.col("log2_fold_change_value") >= 0)
            .then(pl.lit("expression_upregulated"))
            .otherwise(pl.col("datatype_id"))
            .alias("relation_type"),
            pl.col("target_id").alias("x_id"),
            pl.lit("gene").alias("x_type"),
            pl.col("datasource_id").alias("x_source"),
            pl.col("disease_id").alias("y_id"),
            pl.lit("disease").alias("y_type"),
            pl.col("datasource_id").alias("y_source"),
        )
        .join(
            targets.select(["id", "symbol"]), left_on="x_id", right_on="id", how="left"
        )
        .rename({"symbol": "x_name"})
        .join(
            diseases.select(["id", "name"]), left_on="y_id", right_on="id", how="left"
        )
        .rename({"name": "y_name"})
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
                "log2_fold_change_value",
            ]
        )
        .unique(subset=["x_id", "y_id"])
    )

    disgenet_disease_protein = (
        disgenet_diseases.join(
            umls_mondo, left_on="disease_id", right_on="umls_id", how="inner"
        )
        .join(mondo_terms, left_on="mondo_id", right_on="id", how="left")
        .rename(
            {
                "gene_id": "x_id",
                "gene_symbol": "x_name",
                "mondo_id": "y_id",
                "name": "y_name",
            }
        )
        .with_columns(
            [
                pl.lit("gene").alias("x_type"),
                pl.lit("DISGENET").alias("x_source"),
                pl.lit("disease").alias("y_type"),
                pl.lit("MONDO").alias("y_source"),
                pl.lit("disease_protein").alias("relation"),
                pl.lit("associated with").alias("relation_type"),
                pl.col("dsi").cast(pl.Float64).alias("disease_specificity_index"),
                pl.col("dpi").cast(pl.Float64).alias("disease_pleiotropy_index"),
                pl.col("ei").cast(pl.Float64).alias("evidence_index"),
                pl.col("nof_pmids")
                .cast(pl.Int16)
                .alias(
                    "number_of_pmids"
                ),  # Number of PMIDs supporting this association.
                pl.col("nof_snps")
                .cast(pl.Int16)
                .alias("number_of_snps"),  # Number of SNPs supporting this association.
                pl.col("score")
                .cast(pl.Float64)
                .alias(
                    "disgenet_score"
                ),  # Custom disgenet score defined in https://disgenet.com/About#metrics
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
                # NOTE: metadata
                "disease_specificity_index",
                "disease_pleiotropy_index",
                "evidence_index",
                "number_of_pmids",
                "number_of_snps",
                "disgenet_score",
            ]
        )
        .unique(subset=["x_id", "y_id"])
    )

    disease_protein = pl.concat(
        [opentargets_disease_protein, disgenet_disease_protein], how="diagonal"
    )

    # NOTE: We need to merge the same (gene, disease) pairs that have (obviously) the same name/symbol, but different IDs.
    # We keep only one of them, and aggregate the metadata columns from opentargets and disgenet.
    #
    # Example:
    # - (x_name:SCO2, y_name:cardioencephalomyopathy) but (x_id:ENSG00000123456, y_id:MONDO_0011451) and (x_id:NCBIGene:9997, y_id:MONDO_0011451)
    # See: ENSG00000123456 and NCBIGene:9997 are different IDs for the same gene.

    keys = ["x_name", "y_name"]
    metadata_cols = [c for c in disease_protein.columns if c not in keys]
    agg_exprs = []
    for column_name in metadata_cols:
        agg_exprs.append(
            pl.col(column_name)
            .filter(pl.col(column_name).is_not_null())
            .first()
            .alias(column_name)
        )
    disease_protein = disease_protein.group_by(keys).agg(agg_exprs)
    return disease_protein.select([
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
        "log2_fold_change_value",
        "disease_specificity_index",
        "disease_pleiotropy_index", 
        "evidence_index",
        "number_of_pmids",
        "number_of_snps",
        "disgenet_score"
    ]).sort(by=["x_id", "y_id"])


disease_protein_node = node(
    run,
    inputs={
        # opentargets
        "evidence": "bronze.opentargets.evidence",
        "targets": "bronze.opentargets.targets",
        "diseases": "bronze.opentargets.diseases",
        "disease_phenotype_ids": "bronze.opentargets.disease_phenotype_ids",
        "score_threshold": "params:score_threshold",
        # disgenet
        "disgenet_diseases": "bronze.disgenet.diseases",
        "umls_mondo": "silver.umls.umls_mondo",
        "mondo_terms": "bronze.ontology.mondo_terms",
    },
    outputs="disease_protein",
    name="disease_protein",
    tags=["silver"],
)
