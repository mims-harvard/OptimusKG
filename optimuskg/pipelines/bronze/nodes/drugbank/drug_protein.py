import polars as pl
from kedro.pipeline import node


def to_dict_mapping(
    df: pl.DataFrame, key_col: str, val_col: str
) -> dict[str, str | float]:
    """
    Convert two columns of a Polars DataFrame to a Python dict:
      key_col -> val_col
    Similar to Pandas 'set_index(...).to_dict()[...]' usage.
    """
    # polars .to_dicts() returns a list of row-wise dicts
    rows = df.select([key_col, val_col]).drop_nulls().to_dicts()
    return {row[key_col]: row[val_col] for row in rows}


def add_col(
    df: pl.DataFrame, mapping: dict[str, str | float], source_col: str, new_col: str
) -> pl.DataFrame:
    """
    Return a new DataFrame with an added column based on a dictionary lookup
    from 'source_col' -> 'mapping'. Missing keys become NaN/None.
    """
    return df.with_columns(
        pl.col(source_col)
        .map_elements(lambda x: mapping.get(x, ""), return_dtype=pl.Utf8)
        .alias(new_col)
    )


def drugbank2edges(df: pl.DataFrame, relation: str) -> list[dict]:
    """
    For each row, split the 'Drug IDs' on '; ' to get multiple DrugBank IDs,
    then build a list of dictionaries with the relevant fields.
    """
    edges = []
    for row in df.to_dicts():
        drug_ids = row["Drug IDs"].split("; ")
        for drug in drug_ids:
            edges.append(
                {
                    "DrugBank": drug,
                    "relation": relation,
                    "NCBIGeneID": row["NCBIGeneID"],
                    "UniProtName": row["Name"],
                    "UniProtID": row["UniProt ID"],
                }
            )
    return edges


def process_drug_protein(  # noqa: PLR0913
    df_gene_map: pl.DataFrame,
    df_carrier: pl.DataFrame,
    df_enzyme: pl.DataFrame,
    df_target: pl.DataFrame,
    df_transporter: pl.DataFrame,
    df_vocabulary: pl.DataFrame,
) -> pl.DataFrame:
    df_carrier = df_carrier.filter(pl.col("Species") == "Humans")
    df_carrier = df_carrier.filter(pl.col("Species") == "Humans")
    df_enzyme = df_enzyme.filter(pl.col("Species") == "Humans")
    df_target = df_target.filter(pl.col("Species") == "Humans")
    df_transporter = df_transporter.filter(pl.col("Species") == "Humans")

    up2ncbi = to_dict_mapping(
        df_gene_map,
        key_col="UniProt ID(supplied by UniProt)",
        val_col="NCBI Gene ID(supplied by NCBI)",
    )

    df_carrier = add_col(df_carrier, up2ncbi, "UniProt ID", "NCBIGeneID")
    df_enzyme = add_col(df_enzyme, up2ncbi, "UniProt ID", "NCBIGeneID")
    df_target = add_col(df_target, up2ncbi, "UniProt ID", "NCBIGeneID")
    df_transporter = add_col(df_transporter, up2ncbi, "UniProt ID", "NCBIGeneID")

    # Convert each DataFrame to an "edges" list: [DrugBank, relation, NCBIGeneID, ...]
    all_edges = []
    all_edges.extend(drugbank2edges(df_carrier, "carrier"))
    all_edges.extend(drugbank2edges(df_enzyme, "enzyme"))
    all_edges.extend(drugbank2edges(df_target, "target"))
    all_edges.extend(drugbank2edges(df_transporter, "transporter"))

    df_prot_drug = pl.DataFrame(all_edges)

    dbid2name = to_dict_mapping(
        df_vocabulary, key_col="DrugBank ID", val_col="Common name"
    )

    df_prot_drug = add_col(
        df_prot_drug, dbid2name, source_col="DrugBank", new_col="DrugBankName"
    )

    df_prot_drug = df_prot_drug.rename(
        {
            "DrugBank": "drug_bank",
            "relation": "relation",
            "NCBIGeneID": "ncbi_gene_id",
            "UniProtName": "uniprot_name",
            "UniProtID": "uniprot_id",
            "DrugBankName": "drug_bank_name",
        }
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
        "df_vocabulary": "landing.drugbank.vocabulary",
    },
    outputs="drugbank.drug_protein",
    name="drug_protein",
    tags=["bronze"],
)
