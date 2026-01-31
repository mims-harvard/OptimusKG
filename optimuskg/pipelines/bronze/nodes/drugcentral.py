import polars as pl
from kedro.pipeline import node


def run(
    drugcentral: pl.DataFrame,
    drugbank_vocabulary: pl.DataFrame,
    umls_disease_mappings: pl.DataFrame,
    chembl_drugbank_mapping: pl.DataFrame,
) -> pl.DataFrame:
    drugcentral_normalized = (
        drugcentral.filter(
            pl.col("cas_reg_no").is_not_null() & pl.col("umls_cui").is_not_null()
        )
        .join(drugbank_vocabulary, left_on="cas_reg_no", right_on="cas", how="inner")
        .join(
            umls_disease_mappings, left_on="umls_cui", right_on="umls_id", how="inner"
        )
        .join(
            chembl_drugbank_mapping,
            left_on="drugbank_id",
            right_on="drugbank_id",
            how="left",
        )
        .select(  # TODO: I think we can also add the "stem" column here
            pl.coalesce([pl.col("chembl_id"), pl.col("drugbank_id")]).alias("from"),
            pl.col("id").alias("to"),
            pl.col("structure_id").cast(pl.Utf8),
            pl.col("drug_disease_id").cast(pl.Utf8),
            pl.col("cd_id").cast(pl.Utf8),
            pl.col("cd_formula"),
            pl.col("cd_molweight"),
            pl.col("clogp"),
            pl.col("alogs"),
            pl.col("cas_reg_no"),
            pl.col("tpsa"),
            pl.col("lipinski"),
            pl.coalesce([pl.col("common_name"), pl.col("name")]).alias("name"),
            pl.col("no_formulations").cast(pl.Int32),
            pl.col("molfile_base64"),
            pl.col("mrdef"),
            pl.col("enhanced_stereo"),
            pl.col("arom_c").cast(pl.Int32),
            pl.col("sp3_c").cast(pl.Int32),
            pl.col("sp2_c").cast(pl.Int32),
            pl.col("sp_c").cast(pl.Int32),
            pl.col("halogen").cast(pl.Int32),
            pl.col("hetero_sp2_c").cast(pl.Int32),
            pl.col("rotb").cast(pl.Int32),
            pl.col("molimg_base64"),
            pl.col("o_n").cast(pl.Int32),
            pl.col("oh_nh").cast(pl.Int32),
            pl.col("inchi"),
            pl.col("smiles"),
            pl.col("rgb"),
            pl.col("fda_labels").cast(pl.Int32),
            pl.coalesce([pl.col("standard_inchi_key"), pl.col("inchikey")]).alias(
                "inchiKey"
            ),
            pl.col("status"),
            pl.col("struct_id").cast(pl.Utf8),
            pl.col("concept_id").cast(pl.String),
            pl.col("relationship_name"),
            pl.col("concept_name"),
            pl.col("umls_cui"),
            pl.col("snomed_full_name"),
            pl.col("cui_semantic_type"),
            pl.col("snomed_conceptid").cast(pl.String),
            pl.col("accession_numbers"),
            pl.col("unii"),
            pl.col("synonyms"),
        )
        .sort(by=["from", "to"])
        .unique(subset=["from", "to"])
    )

    drugcentral_edges = drugcentral_normalized.select(
        "from", "to", "structure_id", "drug_disease_id", "relationship_name"
    )
    drug_disease = drugcentral_edges.filter(~pl.col("to").str.starts_with("HP_")).sort(
        by=["from", "to"]
    )
    drug_phenotype = drugcentral_edges.filter(pl.col("to").str.starts_with("HP_")).sort(
        by=["from", "to"]
    )

    drug = (
        drugcentral_normalized.select(
            pl.col("from").alias("id"),
            pl.col("cd_id"),
            pl.col("cd_formula"),
            pl.col("cd_molweight"),
            pl.col("clogp"),
            pl.col("alogs"),
            pl.col("cas_reg_no"),
            pl.col("tpsa"),
            pl.col("lipinski"),
            pl.col("name"),
            pl.col("no_formulations"),
            pl.col("molfile_base64"),
            pl.col("mrdef"),
            pl.col("enhanced_stereo"),
            pl.col("arom_c"),
            pl.col("sp3_c"),
            pl.col("sp2_c"),
            pl.col("sp_c"),
            pl.col("halogen"),
            pl.col("hetero_sp2_c"),
            pl.col("rotb"),
            pl.col("molimg_base64"),
            pl.col("o_n"),
            pl.col("oh_nh"),
            pl.col("inchi"),
            pl.col("smiles"),
            pl.col("rgb"),
            pl.col("fda_labels"),
            pl.col("inchiKey"),
            pl.col("status"),
            pl.col("struct_id"),
            pl.col("accession_numbers"),
            pl.col("unii"),
            pl.col("synonyms"),
        )
        .unique()
        .sort(by="id")
    )

    disease_and_phenotype = (
        drugcentral_normalized.select(
            pl.col("to").alias("id"),
            pl.col("concept_id"),
            pl.col("concept_name"),
            pl.col("umls_cui"),
            pl.col("snomed_full_name"),
            pl.col("cui_semantic_type"),
            pl.col("snomed_conceptid"),
        )
        .group_by("id")
        .agg(
            pl.col("concept_id").drop_nulls().unique().sort().alias("concept_ids"),
            pl.col("concept_name").drop_nulls().unique().sort().alias("concept_names"),
            pl.col("umls_cui").first(),
            pl.col("snomed_full_name").drop_nulls().unique().sort().alias("snomed_full_names"),
            pl.col("cui_semantic_type").first(),
            pl.col("snomed_conceptid").drop_nulls().unique().sort().alias("snomed_conceptids"),
        )
        .unique(subset="id")
        .sort(by="id")
    )

    disease = disease_and_phenotype.filter(~pl.col("id").str.starts_with("HP_")).sort(
        by="id"
    )
    phenotype = disease_and_phenotype.filter(pl.col("id").str.starts_with("HP_")).sort(
        by="id"
    )

    return drug_disease, drug_phenotype, drug, phenotype, disease


drugcentral_node = node(
    run,
    inputs={
        "drugcentral": "landing.drugcentral.psql_dump",
        "drugbank_vocabulary": "bronze.drugbank.vocabulary",
        "umls_disease_mappings": "bronze.opentargets.umls_disease_mappings",
        "chembl_drugbank_mapping": "bronze.opentargets.chembl_drugbank_mapping",
    },
    outputs=[
        "drugcentral.drug_disease",
        "drugcentral.drug_phenotype",
        "drugcentral.drug",
        "drugcentral.phenotype",
        "drugcentral.disease",
    ],
    name="drugcentral",
    tags=["bronze"],
)
