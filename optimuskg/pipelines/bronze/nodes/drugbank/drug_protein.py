import polars as pl
from kedro.pipeline import node


def process_drug_protein(  # noqa: PLR0913
    df_gene_map: pl.DataFrame,
    df_carrier: pl.DataFrame,
    df_enzyme: pl.DataFrame,
    df_target: pl.DataFrame,
    df_transporter: pl.DataFrame,
    df_vocabulary: pl.DataFrame,
) -> pl.DataFrame:
    # Prepare gene map for joining
    df_gene_map = df_gene_map.select(
        pl.col("UniProt ID(supplied by UniProt)").alias("uniprot_id"),
        pl.col("NCBI Gene ID(supplied by NCBI)").alias("ncbi_gene_id"),
    ).drop_nulls()

    all_edge_dfs = []
    dataframes_info = [
        (df_carrier, "carrier"),
        (df_enzyme, "enzyme"),
        (df_target, "target"),
        (df_transporter, "transporter"),
    ]

    for df, relation_name in dataframes_info:
        df_filtered = df.filter(pl.col("Species") == "Humans")

        # Add NCBIGeneID using a left join
        df_with_ncbi = df_filtered.join(
            df_gene_map,
            left_on="UniProt ID",
            right_on="uniprot_id",
            how="left",
        ).with_columns(pl.col("ncbi_gene_id").cast(pl.Utf8).fill_null(""))

        edge_df = (
            df_with_ncbi.with_columns(
                pl.col("Drug IDs").str.split(by="; ").alias("DrugBank_list")
            )
            .explode("DrugBank_list")
            .select(
                pl.col("DrugBank_list").alias("drug_bank_id"),
                pl.lit(relation_name).alias("relation"),
                pl.col("ncbi_gene_id"),
                pl.col("Name").alias("uniprot_name"),
                pl.col("UniProt ID").alias("uniprot_id"),
            )
        )
        all_edge_dfs.append(edge_df)

    df_prot_drug = pl.concat(all_edge_dfs, how="diagonal")

    # Prepare vocabulary for joining
    df_vocabulary_join_prep = df_vocabulary.select(
        pl.col("drugbank_id"),
        pl.col("common_name").alias("drug_bank_name"),
    ).drop_nulls()

    # Add DrugBankName using a left join
    df_prot_drug = df_prot_drug.with_columns(
        (pl.lit("DRUGBANK:") + pl.col("drug_bank_id")).alias("drug_bank_id")
    )

    df_prot_drug = df_prot_drug.join(
        df_vocabulary_join_prep,
        left_on="drug_bank_id",
        right_on="drugbank_id",
        how="left",
    ).with_columns(pl.col("drug_bank_name").cast(pl.Utf8).fill_null(""))

    # Add prefixes
    df_prot_drug = df_prot_drug.with_columns(
        (pl.lit("NCBIGene:") + pl.col("ncbi_gene_id")).alias("ncbi_gene_id"),
        (pl.lit("UniProtKB:") + pl.col("uniprot_id")).alias("uniprot_id"),
    )

    return df_prot_drug


drug_protein_node = node(
    process_drug_protein,
    inputs={
        "df_gene_map": "landing.drugbank.gene_map",
        "df_carrier": "landing.drugbank.carrier",
        "df_enzyme": "landing.drugbank.enzyme",
        "df_target": "landing.drugbank.target",
        "df_transporter": "landing.drugbank.transporter",
        "df_vocabulary": "bronze.drugbank.vocabulary",
    },
    outputs="drugbank.drug_protein",
    name="drug_protein",
    tags=["bronze"],
)
