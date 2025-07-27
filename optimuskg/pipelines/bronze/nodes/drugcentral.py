import polars as pl
from kedro.pipeline import node


def run(drugcentral: pl.DataFrame) -> pl.DataFrame:
    return drugcentral.select(["cas_reg_no", "relationship_name", "umls_cui"]).filter(
        ~pl.col("cas_reg_no").is_null() & ~pl.col("umls_cui").is_null()
    )


drugcentral_node = node(
    run,
    inputs={
        "drugcentral": "landing.drugcentral.psql_dump",
    },
    outputs="drug_disease",
    name="drugcentral",
    tags=["bronze"],
)
