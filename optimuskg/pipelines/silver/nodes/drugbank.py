import polars as pl
from kedro.pipeline import node


def process_drugbank(
    drug_drug: pl.DataFrame,
    drug_protein: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    return drug_drug, drug_protein


drugbank_node = node(
    process_drugbank,
    inputs={
        "drug_drug": "bronze.drugbank.drug_drug",
        "drug_protein": "bronze.drugbank.drug_protein",
    },
    outputs=[
        "drugbank.drug_drug",
        "drugbank.drug_protein",
    ],
    name="drugbank",
    tags=["silver"],
)
