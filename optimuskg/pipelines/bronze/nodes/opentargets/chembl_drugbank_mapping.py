import polars as pl
from kedro.pipeline import node


def run(
    drug_molecule: pl.DataFrame,
) -> pl.DataFrame:
    return (
        drug_molecule.unnest("metadata")
        .explode("cross_references")
        .unnest("cross_references")
        .filter(pl.col("source") == "drugbank")
        .explode("ids")
        .unique("id")
        .select(
            [
                pl.col("id").alias("chembl_id"),
                ("DRUGBANK:" + pl.col("ids")).alias("drugbank_id"),
            ]
        )
        .sort(by=["drugbank_id", "chembl_id"])
    )


chembl_drugbank_mapping_node = node(
    run,
    inputs={
        "drug_molecule": "bronze.opentargets.drug_molecule",
    },
    outputs="opentargets.chembl_drugbank_mapping",
    name="chembl_drugbank_mapping",
    tags=["bronze"],
)
