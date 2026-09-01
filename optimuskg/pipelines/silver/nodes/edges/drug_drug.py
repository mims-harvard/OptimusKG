import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import (
    Edge,
    Node,
    Relation,
    Source,
    merge_relation_assertions,
    relation_assertions,
    relation_conflict_expr,
    resolve_relation_expr,
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
                relation_assertions(
                    Source.DRUG_BANK, pl.lit(Relation.SYNERGISTIC_INTERACTION)
                ).alias("relation_assertions"),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.struct(
                            [
                                pl.lit([Source.DRUG_BANK])
                                .cast(pl.List(pl.String))
                                .alias("direct"),
                                pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                            ]
                        ).alias("sources"),
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
                relation_assertions(Source.OPEN_TARGETS, pl.lit(Relation.PARENT)).alias(
                    "relation_assertions"
                ),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.struct(
                            [
                                pl.lit([Source.OPEN_TARGETS])
                                .cast(pl.List(pl.String))
                                .alias("direct"),
                                pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                            ]
                        ).alias("sources"),
                    ]
                ).alias("opentargets_props"),
            ]
        )
    )

    # Union of the DrugBank and OpenTargets assertions for each node pair, so
    # that collapsing to one edge per node pair stays lossless.
    merged_assertions = merge_relation_assertions(
        pl.col("relation_assertions"), pl.col("relation_assertions_right")
    )

    return (
        drugbank_drug_drug.join(opentargets_drug_drug, on=["from", "to"], how="full")
        .select(
            [
                pl.coalesce([pl.col("from"), pl.col("from_right")]).alias("from"),
                pl.coalesce([pl.col("to"), pl.col("to_right")]).alias("to"),
                pl.coalesce([pl.col("label"), pl.col("label_right")]).alias("label"),
                resolve_relation_expr(merged_assertions).alias("relation"),
                pl.coalesce([pl.col("undirected"), pl.col("undirected_right")]).alias(
                    "undirected"
                ),
                pl.struct(
                    [
                        pl.struct(
                            [
                                pl.concat_list(
                                    [
                                        pl.coalesce(
                                            [
                                                pl.col("drugbank_props")
                                                .struct.field("sources")
                                                .struct.field("direct"),
                                                pl.lit([], dtype=pl.List(pl.String)),
                                            ]
                                        ),
                                        pl.coalesce(
                                            [
                                                pl.col("opentargets_props")
                                                .struct.field("sources")
                                                .struct.field("direct"),
                                                pl.lit([], dtype=pl.List(pl.String)),
                                            ]
                                        ),
                                    ]
                                ).alias("direct"),
                                pl.concat_list(
                                    [
                                        pl.coalesce(
                                            [
                                                pl.col("drugbank_props")
                                                .struct.field("sources")
                                                .struct.field("indirect"),
                                                pl.lit([], dtype=pl.List(pl.String)),
                                            ]
                                        ),
                                        pl.coalesce(
                                            [
                                                pl.col("opentargets_props")
                                                .struct.field("sources")
                                                .struct.field("indirect"),
                                                pl.lit([], dtype=pl.List(pl.String)),
                                            ]
                                        ),
                                    ]
                                ).alias("indirect"),
                            ]
                        ).alias("sources"),
                        pl.col("drugbank_props")
                        .struct.field("interaction_description")
                        .alias(
                            "interaction_description"
                        ),  # TODO: change this column name to relation_description and add the column to the opentargets_props with something like "this drug is the parent of the other drug in the ontology"
                        merged_assertions.alias("relation_assertions"),
                        relation_conflict_expr(merged_assertions).alias(
                            "relation_conflict"
                        ),
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
