import polars as pl
from kedro.pipeline import node


def run(
    disease: pl.DataFrame,
) -> pl.DataFrame:
    return (
        disease.select(
            [pl.col("id"), pl.col("metadata").struct.field("dbXRefs").alias("dbXRefs")]
        )
        .explode("dbXRefs")
        .filter(pl.col("dbXRefs").is_not_null())
        .filter(pl.col("dbXRefs").str.starts_with("UMLS"))
        .group_by("id")
        .agg([pl.col("dbXRefs").first().str.replace("UMLS:", "").alias("umls_id")])
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
