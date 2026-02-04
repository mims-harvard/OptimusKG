import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node


def run(
    drug_protein: pl.DataFrame,
    drug_mechanism_of_action: pl.DataFrame,
    chembl_drugbank_mapping: pl.DataFrame,
    ensembl_ncbi_mapping: pl.DataFrame,
) -> pl.DataFrame:
    drugbank_drug_protein = (
        drug_protein.join(
            chembl_drugbank_mapping,
            left_on="drug_bank_id",
            right_on="drugbank_id",
            how="inner",
        )
        .join(
            ensembl_ncbi_mapping,
            left_on="ncbi_gene_id",
            right_on="ncbi_id",
            how="inner",
        )
        .group_by(["chembl_id", "ensembl_id"])
        .agg([pl.col("relation").unique().alias("relation_type")])
        .select(
            pl.col("chembl_id").alias("from"),
            pl.col("ensembl_id").alias("to"),
            pl.lit(Edge.format_label(Node.DRUG, Node.PROTEIN)).alias("label"),
            pl.col("relation_type").alias("relation"),
            pl.lit(False).alias("undirected"),
            pl.struct(
                [
                    pl.lit(["drugbank", "opentargets"]).alias("sources"),
                ]
            ).alias("drugbank_props"),
        )
    )

    opentargets_drug_protein = (
        drug_mechanism_of_action.with_columns(
            pl.col("metadata").struct.field("references"),
            pl.col("metadata").struct.field("action_type"),
        )
        .explode("targets")
        .explode("chembl_ids")
        .explode("references")
        .unnest("references")
        .filter(pl.col("targets").is_not_null())
        .group_by(["targets", "chembl_ids"])
        .agg(
            [
                pl.col("mechanism_of_action")
                .drop_nulls()
                .unique()
                .alias("mechanisms_of_action"),
                pl.col("source").drop_nulls().unique().alias("sources"),
                pl.concat_list("ids")
                .flatten()
                .drop_nulls()
                .unique()
                .alias("source_ids"),
                pl.concat_list("urls")
                .flatten()
                .drop_nulls()
                .unique()
                .alias("source_urls"),
                pl.col("action_type").drop_nulls().unique().alias("action_type"),
            ]
        )
        .select(
            [
                pl.col("chembl_ids").alias("from"),
                pl.col("targets").alias("to"),
                pl.lit(Edge.format_label(Node.DRUG, Node.PROTEIN)).alias("label"),
                pl.col("action_type").alias("relation"),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.col("sources"),
                        pl.col("source_ids"),
                        pl.col("source_urls"),
                        pl.col("mechanisms_of_action"),
                    ]
                ).alias("opentargets_props"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )

    return (
        drugbank_drug_protein.join(
            opentargets_drug_protein, on=["from", "to"], how="left"
        )
        .with_columns(
            [
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
                .list.unique()
                .alias("relation_merged"),
                pl.when(pl.col("opentargets_props").is_not_null())
                .then(
                    pl.struct(
                        [
                            *[
                                pl.col("opentargets_props").struct.field(f)
                                for f in [
                                    "source_ids",
                                    "source_urls",
                                    "mechanisms_of_action",
                                ]
                            ],
                            pl.concat_list(
                                [
                                    pl.col("opentargets_props").struct.field("sources"),
                                    pl.col("drugbank_props").struct.field("sources"),
                                ]
                            )
                            .list.unique()
                            .alias("sources"),
                        ]
                    )
                )
                .otherwise(pl.col("drugbank_props"))
                .alias("properties"),
            ]
        )
        .select(
            [
                "from",
                "to",
                "label",
                pl.col("relation_merged").alias("relation"),
                "undirected",
                "properties",
            ]
        )
        .unique(subset=["from", "to"])
        .sort(["from", "to"])
    )


drug_protein_node = node(
    run,
    inputs={
        "drug_protein": "bronze.drug_protein",
        "drug_mechanism_of_action": "bronze.opentargets.drug_mechanism_of_action",
        "chembl_drugbank_mapping": "bronze.opentargets.chembl_drugbank_mapping",
        "ensembl_ncbi_mapping": "bronze.opentargets.ensembl_ncbi_mapping",
    },
    outputs="edges.drug_protein",
    name="drug_protein",
    tags=["silver"],
)
