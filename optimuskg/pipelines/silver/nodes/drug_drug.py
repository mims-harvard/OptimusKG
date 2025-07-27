import polars as pl
from kedro.pipeline import node


def run(
    drug_drug: pl.DataFrame,
    vocabulary: pl.DataFrame,
) -> pl.DataFrame:
    # Join with vocabulary to get drug names for head drug
    df_drug_drug = drug_drug.join(
        vocabulary, left_on="head_drug_id", right_on="drugbank_id", how="inner"
    )
    df_drug_drug = df_drug_drug.rename(
        {"head_drug_id": "x_id", "common_name": "x_name"}
    )

    # Join with vocabulary to get drug names for tail drug
    df_drug_drug = df_drug_drug.join(
        vocabulary, left_on="tail_drug_id", right_on="drugbank_id", how="inner"
    )
    df_drug_drug = df_drug_drug.rename(
        {"tail_drug_id": "y_id", "common_name": "y_name"}
    )

    # Add type and source columns
    df_drug_drug = df_drug_drug.with_columns(
        [
            pl.lit("drug").alias("x_type"),
            pl.lit("DrugBank").alias("x_source"),
            pl.lit("drug").alias("y_type"),
            pl.lit("DrugBank").alias("y_source"),
            pl.lit("drug_drug").alias("relation"),
            pl.lit("synergistic interaction").alias("relation_type"),
            pl.col("accession_numbers").alias("x_accession_numbers"),
            pl.col("cas").alias("x_cas"),
            pl.col("unii").alias("x_unii"),
            pl.col("synonyms").alias("x_synonyms"),
            pl.col("standard_inchi_key").alias("x_standard_inchi_key"),
            pl.col("accession_numbers_right").alias("y_accession_numbers"),
            pl.col("cas_right").alias("y_cas"),
            pl.col("unii_right").alias("y_unii"),
            pl.col("synonyms_right").alias("y_synonyms"),
            pl.col("standard_inchi_key_right").alias("y_standard_inchi_key"),
        ]
    )

    df_drug_drug = df_drug_drug.select(
        [
            "relation",
            "relation_type",
            "description",
            "x_id",
            "x_type",
            "x_name",
            "x_source",
            "x_accession_numbers",
            "x_cas",
            "x_unii",
            "x_synonyms",
            "x_standard_inchi_key",
            "y_id",
            "y_type",
            "y_name",
            "y_source",
            "y_accession_numbers",
            "y_cas",
            "y_unii",
            "y_synonyms",
            "y_standard_inchi_key",
        ]
    )

    return df_drug_drug


drug_drug_node = node(
    run,
    inputs={
        "drug_drug": "bronze.drug_drug",
        "vocabulary": "bronze.drugbank.vocabulary",
    },
    outputs="drug_drug",
    name="drug_drug",
    tags=["silver"],
)
