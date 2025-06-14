import polars as pl
from kedro.pipeline import node


def process_umls_mondo(
    mrconso: pl.DataFrame,
    mondo_terms: pl.DataFrame,
    mondo_xrefs: pl.DataFrame,
) -> pl.DataFrame: ...


umls_mondo_node = node(
    process_umls_mondo,
    inputs={
        "mrconso": "bronze.umls.mrconso",
        "mondo_terms": "bronze.ontology.mondo_terms",
        "mondo_xrefs": "bronze.ontology.mondo_xrefs",
    },
    outputs="umls.umls_mondo",
    name="umls_mondo",
    tags=["silver"],
)
