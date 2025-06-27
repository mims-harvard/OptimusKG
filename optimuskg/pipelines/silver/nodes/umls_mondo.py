import polars as pl
from kedro.pipeline import node


def run(
    mrconso: pl.DataFrame,
    mondo_xrefs: pl.DataFrame,
) -> pl.DataFrame:
    return (
        mrconso.join(
            mondo_xrefs, left_on="source_code", right_on="xref_id", how="inner"
        )
        .with_columns(pl.col("id").alias("mondo_id"), pl.col("cui").alias("umls_id"))
        .select(["mondo_id", "umls_id"])
    )


umls_mondo_node = node(
    run,
    inputs={
        "mrconso": "bronze.umls.mrconso",
        "mondo_xrefs": "bronze.ontology.mondo_xrefs",
    },
    outputs="umls.umls_mondo",
    name="umls_mondo",
    tags=["silver"],
)
