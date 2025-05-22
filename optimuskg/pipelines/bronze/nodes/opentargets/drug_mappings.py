from collections.abc import Callable
from typing import Any

import polars as pl
from kedro.pipeline import node

from .utils import concat_json_partitions


def process_drug_mappings(
    drug_mappings_df: pl.DataFrame,
    molecule: dict[str, Callable[[], Any]],
) -> pl.DataFrame:
    ot_drugs_df = concat_json_partitions(molecule).select(
        pl.col("id").cast(pl.String),
        pl.col("name").cast(pl.String),
        pl.col("description").cast(pl.String),
    )

    chembl_to_drop = [
        "CHEMBL440464",
        "CHEMBL1530",
        "CHEMBL8663",
        "CHEMBL261850",
        "CHEMBL148530",
        "CHEMBL599035",
        "CHEMBL1650559",
    ]

    # Split into separate lines for debugging purposes
    df = drug_mappings_df.select(["drugbankId", "chembl_id"])
    df = df.drop_nulls(subset=["drugbankId", "chembl_id"])
    df = df.unique(subset=["drugbankId", "chembl_id"])
    df = df.filter(~pl.col("chembl_id").is_in(chembl_to_drop))
    df = df.rename({"chembl_id": "id"})
    df = df.join(
        ot_drugs_df,
        on="id",
        how="inner",
    )

    df = df.sort(by=sorted(df.columns))
    return df


drug_mappings_node = node(
    process_drug_mappings,
    inputs={
        "drug_mappings_df": "landing.opentargets.drug_mappings",
        "molecule": "landing.opentargets.molecule",
    },
    outputs="opentargets.drug_mappings",
    name="drug_mappings",
    tags=["bronze"],
)
