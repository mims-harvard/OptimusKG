import polars as pl
from kedro.pipeline import node


def run(
    drug_drug: pl.DataFrame,
    drug_molecule: pl.DataFrame,
    chembl_drugbank_mapping: pl.DataFrame,
) -> pl.DataFrame:
    drugbank_drug_drug = (
        drug_drug.join(
            chembl_drugbank_mapping,
            left_on="tail_drug_id",
            right_on="drugbank_id",
            how="left",
        )
        .rename({"chembl_id": "tail_chembl_id"})
        .join(
            chembl_drugbank_mapping,
            left_on="head_drug_id",
            right_on="drugbank_id",
            how="left",
        )
        .rename({"chembl_id": "head_chembl_id"})
        .sort(by=["tail_chembl_id", "head_chembl_id"])
        .select(
            [
                pl.when(pl.col("tail_chembl_id").is_not_null())
                .then(pl.col("tail_chembl_id"))
                .otherwise(pl.col("tail_drug_id"))
                .alias("from"),
                pl.when(pl.col("head_chembl_id").is_not_null())
                .then(pl.col("head_chembl_id"))
                .otherwise(pl.col("head_drug_id"))
                .alias("to"),
                pl.lit("drug_drug").alias("relation"),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.lit(["synergistic interaction"]).alias("relationType"),
                        pl.lit(["drugbank"]).alias("sources"),
                        pl.col("description").alias("interactionDescription"),
                    ]
                ).alias("drugbank_properties"),
            ]
        )
    )

    opentargets_drug_drug = (
        drug_molecule.unnest("metadata")
        .explode("childChemblIds")
        .filter(pl.col("childChemblIds").is_not_null())
        .select(
            [
                pl.col("id").alias("from"),
                pl.col("childChemblIds").alias("to"),
                pl.lit("drug_drug").alias("relation"),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.lit(["parent"]).alias("relationType"),
                        pl.lit(["opentargets"]).alias("sources"),
                    ]
                ).alias("opentargets_properties"),
            ]
        )
    )

    return (
        drugbank_drug_drug.join(opentargets_drug_drug, on=["from", "to"], how="full")
        .select(
            [
                pl.coalesce([pl.col("from"), pl.col("from_right")]).alias("from"),
                pl.coalesce([pl.col("to"), pl.col("to_right")]).alias("to"),
                pl.coalesce([pl.col("relation"), pl.col("relation_right")]).alias(
                    "relation"
                ),
                pl.coalesce([pl.col("undirected"), pl.col("undirected_right")]).alias(
                    "undirected"
                ),
                pl.struct(
                    [
                        pl.concat_list(
                            [
                                pl.coalesce(
                                    [
                                        pl.col("drugbank_properties").struct.field(
                                            "relationType"
                                        ),
                                        pl.lit([], dtype=pl.List(pl.Utf8)),
                                    ]
                                ),
                                pl.coalesce(
                                    [
                                        pl.col("opentargets_properties").struct.field(
                                            "relationType"
                                        ),
                                        pl.lit([], dtype=pl.List(pl.Utf8)),
                                    ]
                                ),
                            ]
                        ).alias("relationType"),
                        pl.concat_list(
                            [
                                pl.coalesce(
                                    [
                                        pl.col("drugbank_properties").struct.field(
                                            "sources"
                                        ),
                                        pl.lit([], dtype=pl.List(pl.Utf8)),
                                    ]
                                ),
                                pl.coalesce(
                                    [
                                        pl.col("opentargets_properties").struct.field(
                                            "sources"
                                        ),
                                        pl.lit([], dtype=pl.List(pl.Utf8)),
                                    ]
                                ),
                            ]
                        ).alias("sources"),
                        pl.col("drugbank_properties")
                        .struct.field("interactionDescription")
                        .alias(
                            "interactionDescription"
                        ),  # TODO: change this column name to relationDescription and add the column to the opentargets_properties with something like "this drug is the parent of the other drug in the ontology"
                    ]
                ).alias("properties"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


drug_drug_node = node(
    run,
    inputs={
        "drug_drug": "bronze.drug_drug",
        "drug_molecule": "bronze.opentargets.drug_molecule",
        "chembl_drugbank_mapping": "bronze.opentargets.chembl_drugbank_mapping",
    },
    outputs="edges.drug_drug",
    name="drug_drug",
    tags=["silver"],
)
