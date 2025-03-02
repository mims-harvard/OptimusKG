import logging

import pandas as pd
import polars as pl
from kedro.pipeline import node

log = logging.getLogger(__name__)


def process_disease_to_phenotype(
    disease_to_phenotype: pd.DataFrame, phenotypes: pl.DataFrame, diseases: pl.DataFrame
) -> pl.DataFrame:
    df = pl.from_pandas(disease_to_phenotype)
    df = df.select("disease", "phenotype")
    df = df.filter(pl.col("disease").is_in(diseases["id"]))
    df = df.filter(pl.col("phenotype").is_in(phenotypes["id"]))
    df = df.join(
        diseases.select(["id", "node_index"]),
        left_on="disease",
        right_on="id",
        how="left",
    )
    df = df.join(
        phenotypes.select(["id", "node_index"]),
        left_on="phenotype",
        right_on="id",
        how="left",
    )
    df = df.rename(
        {"node_index": "disease_index", "node_index_right": "phenotype_index"}
    )

    open_targets_associations = (
        df.with_columns(
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

    return open_targets_associations


disease_to_phenotype_node = node(
    process_disease_to_phenotype,
    inputs={
        "disease_to_phenotype": "bronze.opentargets.disease_to_phenotype",
        "diseases": "bronze.opentargets.diseases",
        "phenotypes": "bronze.opentargets.phenotypes",
    },
    outputs="opentargets.disease_to_phenotype",
    name="disease_to_phenotype",
)
