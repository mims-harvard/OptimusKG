import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.utils import clean_edges


def process_drugbank_drug_drug_interactions(
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
            pl.lit("synergistic interaction").alias("display_relation"),
        ]
    )

    # Clean edges
    df_drug_drug = clean_edges(df_drug_drug)

    return df_drug_drug


drugbank_drug_drug_interactions_node = node(
    process_drugbank_drug_drug_interactions,
    inputs={
        "drug_drug": "bronze.drugbank.drug_drug",
        "vocabulary": "bronze.drugbank.vocabulary",
    },
    outputs="drugbank.drug_drug",
    name="drugbank_drug_drug_interactions",
    tags=["silver"],
)
