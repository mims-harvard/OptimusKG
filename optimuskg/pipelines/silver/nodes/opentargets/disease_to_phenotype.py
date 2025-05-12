import pandas as pd
import polars as pl
from kedro.pipeline import node


def process_disease_to_phenotype(
    disease_to_phenotype: pd.DataFrame, phenotypes: pl.DataFrame, diseases: pl.DataFrame
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
        .filter(pl.col("phenotype").is_in(phenotypes["id"]))
        .join(
            diseases.select(["id", "node_index"]),
            left_on="disease",
            right_on="id",
            how="left",
        )
        .join(
            phenotypes.select(["id", "node_index"]),
            left_on="phenotype",
            right_on="id",
            how="left",
        )
        .rename(
            {
                "node_index": "disease_index",
                "node_index_right": "phenotype_index",
            }
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("disease_index").cast(pl.Utf8),
                    pl.lit("_"),
                    pl.col("phenotype_index").cast(pl.Utf8),
                ]
            ).alias("association")
        )
        .select("association")
        .unique()
    )
    df = df.sort(by=sorted(df.columns))
    return df


disease_to_phenotype_node = node(
    process_disease_to_phenotype,
    inputs={
        "disease_to_phenotype": "bronze.opentargets.disease_to_phenotype",
        "diseases": "bronze.opentargets.diseases",
        "phenotypes": "bronze.opentargets.phenotypes",
    },
    outputs="opentargets.disease_to_phenotype",
    name="disease_to_phenotype",
    tags=["silver"],
)
