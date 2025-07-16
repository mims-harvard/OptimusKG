import polars as pl
from kedro.pipeline import node


def run(hpo_terms: pl.DataFrame, diseases: pl.DataFrame) -> pl.DataFrame:
    df = pl.concat([diseases.select("id"), hpo_terms.select("id")])
    df = df.sort(by=sorted(df.columns))
    return df


disease_phenotype_ids_node = node(
    run,
    inputs={
        "hpo_terms": "ontology.hpo_terms",
        "diseases": "opentargets.diseases",
    },
    outputs="opentargets.disease_phenotype_ids",
    name="disease_phenotype_ids",
    tags=["bronze"],
)
