import polars as pl
from kedro.pipeline import node


def process_drugcentral(
    drug_disease: pl.DataFrame,
) -> pl.DataFrame:
    return drug_disease


drugcentral_node = node(
    process_drugcentral,
    inputs={"drug_disease": "bronze.drugcentral.drug_disease"},
    outputs="drugcentral.drug_disease",
    name="drugcentral",
)
