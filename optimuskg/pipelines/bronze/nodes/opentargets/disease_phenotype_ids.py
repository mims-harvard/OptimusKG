import polars as pl
from kedro.pipeline import node


def process_disease_phenotype_ids(
    phenotypes: pl.DataFrame, diseases: pl.DataFrame
) -> pl.DataFrame:
    df = pl.concat([diseases.select("id"), phenotypes.select("id")])
    df = df.sort(by=sorted(df.columns))
    return df


disease_phenotype_ids_node = node(
    process_disease_phenotype_ids,
    inputs={
        "phenotypes": "opentargets.phenotypes",
        "diseases": "opentargets.diseases",
    },
    outputs="opentargets.disease_phenotype_ids",
    name="disease_phenotype_ids",
    tags=["bronze"],
)
