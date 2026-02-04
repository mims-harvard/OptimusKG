import polars as pl
from kedro.pipeline import node


def run(
    disease: pl.DataFrame,
) -> pl.DataFrame:
    return (
        disease.select(
            [
                pl.col("id"),
                pl.col("metadata").struct.field("db_xrefs").alias("db_xrefs"),
            ]
        )
        .explode("db_xrefs")
        .filter(pl.col("db_xrefs").is_not_null())
        .filter(pl.col("db_xrefs").str.starts_with("UMLS"))
        .group_by("id")
        .agg([pl.col("db_xrefs").first().str.replace("UMLS:", "").alias("umls_id")])
        .sort("umls_id")
    )


umls_disease_mappings_node = node(
    run,
    inputs={
        "disease": "bronze.opentargets.disease",
    },
    outputs="opentargets.umls_disease_mappings",
    name="umls_disease_mappings",
    tags=["bronze"],
)
