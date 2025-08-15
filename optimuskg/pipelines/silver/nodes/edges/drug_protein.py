import polars as pl
from kedro.pipeline import node


def run(
    drug_protein: pl.DataFrame,
    drug_molecule: pl.DataFrame,
    protein_names: pl.DataFrame,
    target: pl.DataFrame,
    drug_mechanism_of_action: pl.DataFrame,
) -> pl.DataFrame:
    ensembl_ncbi_mapping = (
        target.select("id", "approved_symbol")
        .join(protein_names, left_on="approved_symbol", right_on="symbol", how="inner")
        .select(pl.col("id").alias("ensembl_id"), pl.col("ncbi_id"))
    )

    chembl_drugbank_mapping = (
        drug_molecule.unnest("metadata")
        .explode("crossReferences")
        .unnest("crossReferences")
        .filter(pl.col("source") == "drugbank")
        .explode("ids")
        .unique("id")
        .select(
            [
                pl.col("id").alias("chembl_id"),
                ("DRUGBANK:" + pl.col("ids")).alias("drugbank_id"),
            ]
        )
    )

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
        .agg([pl.col("relation").unique().alias("relationType")])
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
                .alias("mechanismsOfAction"),
                pl.col("source").drop_nulls().unique().alias("sources"),
                pl.concat_list("ids")
                .flatten()
                .drop_nulls()
                .unique()
                .alias("sourceIds"),
                pl.concat_list("urls")
                .flatten()
                .drop_nulls()
                .unique()
                .alias("sourceUrls"),
                pl.col("actionType").drop_nulls().unique().alias("actionType"),
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
        "drug_molecule": "bronze.opentargets.drug_molecule",
        "protein_names": "bronze.gene_names.protein_names",
        "target": "bronze.opentargets.target",
        "drug_mechanism_of_action": "bronze.opentargets.drug_mechanism_of_action",
    },
    outputs="edges.drug_protein",
    name="drug_protein",
    tags=["silver"],
)
