import logging
from typing import Final, final

import polars as pl
from kedro.pipeline import node
from typedframe import PolarsTypedFrame

from .utils import KGNodeSchema

log = logging.getLogger(__name__)


@final
class DrugMappingSchema(PolarsTypedFrame):
    schema: Final[dict[str, type[pl.DataType]]] = {
        "drugbankId": pl.String,
        "name": pl.String,
        "ttd_id": pl.String,
        "pubchem_cid": pl.String,
        "cas_num": pl.String,
        "chembl_id": pl.String,
        "zinc_id": pl.String,
        "chebi_id": pl.String,
        "kegg_cid": pl.String,
        "kegg_id": pl.String,
        "bindingDB_id": pl.String,
        "UMLS_cuis": pl.String,
        "stitch_id": pl.String,
    }


def process_drug_mappings(
    drug_mappings: pl.DataFrame, primekg_nodes: pl.DataFrame
) -> pl.DataFrame:
    drug_mappings_df = DrugMappingSchema.convert(drug_mappings).df
    primekg_nodes_df = KGNodeSchema.convert(primekg_nodes).df

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
    return drug_mappings_df  # type: ignore[no-any-return]


drug_mappings_node = node(
    process_drug_mappings,
    inputs={
        "drug_mappings": "landing.opentargets.drug_mappings",
        "primekg_nodes": "landing.opentargets.primekg_nodes",
    },
    outputs="opentargers.drug_mappings",
    name="drug_mappings",
)
