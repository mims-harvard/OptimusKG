import polars as pl
from kedro.pipeline import node


def process_drug_disease(drugcentral: pl.DataFrame) -> pl.DataFrame:
    # Select only the required columns
    df_drug_central = drugcentral.select(
        ["cas_reg_no", "relationship_name", "umls_cui"]
    )

    # Filter out rows where cas_reg_no is null
    df_drug_central = df_drug_central.filter(~pl.col("cas_reg_no").is_null())

    # Filter out rows where umls_cui is null
    df_drug_central = df_drug_central.filter(~pl.col("umls_cui").is_null())

    return df_drug_central


drugcentral_node = node(
    process_drug_disease,
    inputs={
        "drugcentral": "landing.drugcentral.psql_dump",
    },
    outputs="drugcentral.drug_disease",
    name="drugcentral",
    tags=["bronze"],
)
