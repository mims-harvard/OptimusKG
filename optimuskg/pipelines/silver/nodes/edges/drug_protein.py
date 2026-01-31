import polars as pl
from kedro.pipeline import node


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
        .agg([pl.col("relation").unique().sort().alias("relationType")])
        .select(
            pl.col("chembl_id").alias("from"),
            pl.col("ensembl_id").alias("to"),
            pl.lit("drug_protein").alias("relation"),
            pl.lit(False).alias("undirected"),
            pl.struct(
                [
                    pl.lit(["drugbank", "opentargets"]).alias("sources"),
                    pl.col("relationType"),
                ]
            ).alias("drugbank_properties"),
        )
    )

    opentargets_drug_protein = (
        drug_mechanism_of_action.with_columns(
            pl.col("metadata").struct.field("references"),
            pl.col("metadata").struct.field("actionType"),
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
                .sort()
                .alias("mechanismsOfAction"),
                pl.col("source").drop_nulls().unique().sort().alias("sources"),
                pl.concat_list("ids")
                .flatten()
                .drop_nulls()
                .unique()
                .sort()
                .alias("sourceIds"),
                pl.concat_list("urls")
                .flatten()
                .drop_nulls()
                .unique()
                .sort()
                .alias("sourceUrls"),
                pl.col("actionType").drop_nulls().unique().sort().alias("actionType"),
            ]
        )
        .select(
            [
                pl.col("chembl_ids").alias("from"),
                pl.col("targets").alias("to"),
                pl.lit("drug_protein").alias("relation"),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.col("sources"),
                        pl.col("sourceIds"),
                        pl.col("sourceUrls"),
                        pl.col("mechanismsOfAction"),
                        pl.col("actionType").alias("relationType"),
                    ]
                ).alias("opentargets_properties"),
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
                pl.when(pl.col("opentargets_properties").is_not_null())
                .then(
                    pl.struct(
                        [
                            *[
                                pl.col("opentargets_properties").struct.field(f)
                                for f in [
                                    "sourceIds",
                                    "sourceUrls",
                                    "mechanismsOfAction",
                                ]
                            ],
                            pl.concat_list(
                                [
                                    pl.col("opentargets_properties").struct.field(
                                        "sources"
                                    ),
                                    pl.col("drugbank_properties").struct.field(
                                        "sources"
                                    ),
                                ]
                            )
                            .list.unique()
                            .list.sort()
                            .alias("sources"),
                            pl.concat_list(
                                [
                                    pl.col("opentargets_properties").struct.field(
                                        "relationType"
                                    ),
                                    pl.col("drugbank_properties").struct.field(
                                        "relationType"
                                    ),
                                ]
                            )
                            .list.unique()
                            .list.sort()
                            .alias("relationType"),
                        ]
                    )
                )
                .otherwise(pl.col("drugbank_properties"))
                .alias("properties")
            ]
        )
        .select(["from", "to", "relation", "undirected", "properties"])
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
