import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import (
    Edge,
    Node,
    Relation,
    resolve_relation,
)


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
                pl.lit(Edge.format_label(Node.DRUG, Node.DRUG)).alias("label"),
                pl.lit([Relation.SYNERGISTIC_INTERACTION]).alias("relation"),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.lit(["drugbank"]).alias("direct_sources"),
                        pl.lit([]).cast(pl.List(pl.String)).alias("indirect_sources"),
                        pl.col("description").alias("interaction_description"),
                    ]
                ).alias("drugbank_props"),
            ]
        )
    )

    opentargets_drug_drug = (
        drug_molecule.unnest("metadata")
        .explode("child_chembl_ids")
        .filter(pl.col("child_chembl_ids").is_not_null())
        .select(
            [
                pl.col("id").alias("from"),
                pl.col("child_chembl_ids").alias("to"),
                pl.lit(Edge.format_label(Node.DRUG, Node.DRUG)).alias("label"),
                pl.lit([Relation.PARENT]).alias("relation"),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.lit(["opentargets"]).alias("direct_sources"),
                        pl.lit([]).cast(pl.List(pl.String)).alias("indirect_sources"),
                    ]
                ).alias("opentargets_props"),
            ]
        )
    )

    return (
        drugbank_drug_drug.join(opentargets_drug_drug, on=["from", "to"], how="full")
        .select(
            [
                pl.coalesce([pl.col("from"), pl.col("from_right")]).alias("from"),
                pl.coalesce([pl.col("to"), pl.col("to_right")]).alias("to"),
                pl.coalesce([pl.col("label"), pl.col("label_right")]).alias("label"),
                pl.concat_list(
                    [
                        pl.coalesce(
                            [
                                pl.col("relation"),
                                pl.lit([], dtype=pl.List(pl.Utf8)),
                            ]
                        ),
                        pl.coalesce(
                            [
                                pl.col("relation_right"),
                                pl.lit([], dtype=pl.List(pl.Utf8)),
                            ]
                        ),
                    ]
                )
                .map_elements(resolve_relation, return_dtype=pl.Utf8)
                .alias("relation"),
                pl.coalesce([pl.col("undirected"), pl.col("undirected_right")]).alias(
                    "undirected"
                ),
                pl.struct(
                    [
                        pl.concat_list(
                            [
                                pl.coalesce(
                                    [
                                        pl.col("drugbank_props").struct.field(
                                            "direct_sources"
                                        ),
                                        pl.lit([], dtype=pl.List(pl.Utf8)),
                                    ]
                                ),
                                pl.coalesce(
                                    [
                                        pl.col("opentargets_props").struct.field(
                                            "direct_sources"
                                        ),
                                        pl.lit([], dtype=pl.List(pl.Utf8)),
                                    ]
                                ),
                            ]
                        ).alias("direct_sources"),
                        pl.concat_list(
                            [
                                pl.coalesce(
                                    [
                                        pl.col("drugbank_props").struct.field(
                                            "indirect_sources"
                                        ),
                                        pl.lit([], dtype=pl.List(pl.Utf8)),
                                    ]
                                ),
                                pl.coalesce(
                                    [
                                        pl.col("opentargets_props").struct.field(
                                            "indirect_sources"
                                        ),
                                        pl.lit([], dtype=pl.List(pl.Utf8)),
                                    ]
                                ),
                            ]
                        ).alias("indirect_sources"),
                        pl.col("drugbank_props")
                        .struct.field("interaction_description")
                        .alias(
                            "interaction_description"
                        ),  # TODO: change this column name to relation_description and add the column to the opentargets_props with something like "this drug is the parent of the other drug in the ontology"
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
