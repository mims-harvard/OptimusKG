import polars as pl
from kedro.pipeline import node


def process_drugcentral(
    drug_disease: pl.DataFrame,
) -> pl.DataFrame:
    # TODO: pending implementation see: https://github.com/mims-harvard/primekg-2/blob/main/OLD/knowledge_graph/build_graph.ipynb, "Drug disease interactions (DiseaseCentral) –– PENDING"
    return drug_disease


drugcentral_node = node(
    process_drugcentral,
    inputs={"drug_disease": "bronze.drugcentral.drug_disease"},
    outputs="drugcentral.drug_disease",
    name="drugcentral",
    tags=["silver"],
)
