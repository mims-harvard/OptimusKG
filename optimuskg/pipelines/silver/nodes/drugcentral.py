import polars as pl
from kedro.pipeline import node


def run(
    drug_disease: pl.DataFrame,
    umls_mondo: pl.DataFrame,
    mondo_terms: pl.DataFrame,
    vocabulary: pl.DataFrame,
) -> pl.DataFrame:
    return (
        drug_disease.join(vocabulary, left_on="cas_reg_no", right_on="cas", how="left")
        .join(umls_mondo, left_on="umls_cui", right_on="umls_id", how="inner")
        .join(mondo_terms, left_on="mondo_id", right_on="id", how="left")
        .select(["relationship_name", "drugbank_id", "common_name", "mondo_id", "name"])
        .unique()
        .rename(
            {
                "drugbank_id": "x_id",
                "mondo_id": "y_id",
                "common_name": "x_name",
                "name": "y_name",
                "relationship_name": "relation",
            }
        )
        .with_columns(
            [
                pl.lit("drug").alias("x_type"),
                pl.lit("DrugBank").alias("x_source"),
                pl.lit("disease").alias("y_type"),
                pl.lit("MONDO").alias("y_source"),
                pl.lit("drug_disease").alias("relation"),
                pl.when(pl.col("relation") == "off-label use")
                .then(pl.lit("off_label_use"))
                .when(pl.col("relation") == "indication")
                .then(pl.lit("indication"))
                .when(pl.col("relation") == "contraindication")
                .then(pl.lit("contraindication"))
                .alias("relation_type"),
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
    )


drugcentral_node = node(
    run,
    inputs={
        "drug_disease": "bronze.drugcentral.drug_disease",
        "umls_mondo": "silver.umls.umls_mondo",
        "mondo_terms": "bronze.ontology.mondo_terms",
        "vocabulary": "bronze.drugbank.vocabulary",
    },
    outputs="drugcentral.drug_disease",
    name="drugcentral",
    tags=["silver"],
)
