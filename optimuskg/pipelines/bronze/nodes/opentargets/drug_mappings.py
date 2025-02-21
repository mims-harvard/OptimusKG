import logging
from collections.abc import Callable
from typing import Any, Final, final

import polars as pl
from kedro.pipeline import node
from .utils import concat_json_partitions

log = logging.getLogger(__name__)


def process_drug_mappings(
    drug_mappings_df: pl.DataFrame,
    primekg_nodes_df: pl.DataFrame,
    molecule: dict[str, Callable[[], Any]],
) -> pl.DataFrame:
    molecule_df = concat_json_partitions(molecule)
    ot_drugs_df = molecule_df.select(
        pl.col("id").cast(pl.String),
        pl.col("name").cast(pl.String), 
        pl.col("description").cast(pl.String)
    )

    drug_mappings_df = drug_mappings_df.select(["drugbankId", "chembl_id"]).unique()
    drug_mappings_df = drug_mappings_df.drop_nulls()

    drug_mappings_df = drug_mappings_df.join(
        primekg_nodes_df.filter(pl.col("node_type") == "drug"),
        left_on="drugbankId",
        right_on="node_id",
        how="inner",
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
    drug_mappings_df = drug_mappings_df.filter(
        ~pl.col("chembl_id").is_in(chembl_to_drop)
    )
    drug_mappings_df = drug_mappings_df.rename({"chembl_id": "id"})

    drug_mappings_df = drug_mappings_df.join(ot_drugs_df, on="id", how="inner")
    return drug_mappings_df  # type: ignore[no-any-return]


drug_mappings_node = node(
    process_drug_mappings,
    inputs={
        "drug_mappings_df": "landing.opentargets.drug_mappings",
        "primekg_nodes_df": "landing.opentargets.primekg_nodes",
        "molecule": "landing.opentargets.molecule",
    },
    outputs="opentargets.drug_mappings",
    name="drug_mappings",
)
