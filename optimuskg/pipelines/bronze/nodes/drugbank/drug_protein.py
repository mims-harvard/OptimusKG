import polars as pl
from kedro.pipeline import node


def run(  # noqa: PLR0913
    gene_map: pl.DataFrame,
    carrier: pl.DataFrame,
    enzyme: pl.DataFrame,
    target: pl.DataFrame,
    transporter: pl.DataFrame,
    vocabulary: pl.DataFrame,
) -> pl.DataFrame:
    # Prepare gene map for joining
    gene_map = gene_map.select(
        pl.col("UniProt ID(supplied by UniProt)").alias("uniprot_id"),
        pl.col("NCBI Gene ID(supplied by NCBI)").alias("ncbi_gene_id"),
    ).drop_nulls()

    all_edge_dfs = []
    dataframes_info = [
        (carrier, "carrier"),
        (enzyme, "enzyme"),
        (target, "target"),
        (transporter, "transporter"),
    ]

    for df, relation_name in dataframes_info:
        df_filtered = df.filter(pl.col("Species") == "Humans")

        # Add NCBIGeneID using a left join
        df_with_ncbi = df_filtered.join(
            gene_map,
            left_on="UniProt ID",
            right_on="uniprot_id",
            how="left",
        ).with_columns(pl.col("ncbi_gene_id").cast(pl.String).fill_null(""))

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

    drug_protein = pl.concat(all_edge_dfs, how="diagonal")

    # Prepare vocabulary for joining
    df_vocabulary_join_prep = vocabulary.select(
        pl.col("drugbank_id"),
        pl.col("common_name").alias("drug_bank_name"),
    ).drop_nulls()

    # Add DrugBankName using a left join
    drug_protein = drug_protein.with_columns(
        (pl.lit("DRUGBANK:") + pl.col("drug_bank_id")).alias("drug_bank_id")
    )

    drug_protein = drug_protein.join(
        df_vocabulary_join_prep,
        left_on="drug_bank_id",
        right_on="drugbank_id",
        how="left",
    ).with_columns(pl.col("drug_bank_name").cast(pl.String).fill_null(""))

    # Add prefixes
    drug_protein = drug_protein.with_columns(
        (pl.lit("NCBIGene:") + pl.col("ncbi_gene_id")).alias("ncbi_gene_id"),
        (pl.lit("UniProtKB:") + pl.col("uniprot_id")).alias("uniprot_id"),
    )

    return drug_protein.sort(by=["drug_bank_id", "ncbi_gene_id"])


drug_protein_node = node(
    run,
    inputs={
        "gene_map": "landing.drugbank.gene_map",
        "carrier": "landing.drugbank.carrier",
        "enzyme": "landing.drugbank.enzyme",
        "target": "landing.drugbank.target",
        "transporter": "landing.drugbank.transporter",
        "vocabulary": "bronze.drugbank.vocabulary",
    },
    outputs="drug_protein",
    name="drug_protein",
    tags=["bronze"],
)
