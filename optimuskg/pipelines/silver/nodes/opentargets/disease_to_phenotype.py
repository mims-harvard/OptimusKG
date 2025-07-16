import pandas as pd
import polars as pl
from kedro.pipeline import node


def run(
    disease_to_phenotype: pd.DataFrame, hpo_terms: pl.DataFrame, diseases: pl.DataFrame
) -> pl.DataFrame:
    df = pl.from_pandas(disease_to_phenotype)
    df = (
        df.select(
            [
                "disease",
                "phenotype",
            ]
        )
        .filter(pl.col("disease").is_in(diseases["id"]))
        .filter(pl.col("phenotype").is_in(hpo_terms["id"]))
        .with_columns(
            pl.concat_str(
                [
                    pl.col("disease"),
                    pl.lit("_"),
                    pl.col("phenotype"),
                ]
            ).alias("association")
        )
        .select("association")
        .unique()
    )
    df = df.sort(by=sorted(df.columns))
    return df


disease_to_phenotype_node = node(
    run,
    inputs={
        "disease_to_phenotype": "bronze.opentargets.disease_to_phenotype",
        "diseases": "bronze.opentargets.diseases",
        "hpo_terms": "bronze.ontology.hpo_terms",
    },
    outputs="opentargets.disease_to_phenotype",
    name="disease_to_phenotype",
    tags=["silver"],
)
