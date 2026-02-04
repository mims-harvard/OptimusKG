import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Node


def run(  # noqa: PLR0913
    onsides_high_confidence: pl.DataFrame,
    drugbank_vocabulary: pl.DataFrame,
    drug_molecule: pl.DataFrame,
    drugcentral_drug: pl.DataFrame,
    drug_drug: pl.DataFrame,
    drug_protein: pl.DataFrame,
    drug_disease: pl.DataFrame,
    drug_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                drug_drug.with_columns(
                    [pl.col("properties").struct.field("sources").alias("sources")]
                )
                .unpivot(index=["sources"], on=["from", "to"], value_name="id")
                .select(["id", "sources"]),
                drug_disease.select(
                    [
                        pl.col("from").alias("id"),
                        pl.col("properties").struct.field("sources").alias("sources"),
                    ]
                ),
                drug_phenotype.select(
                    [
                        pl.col("from").alias("id"),
                        pl.col("properties").struct.field("sources").alias("sources"),
                    ]
                ),
                drug_protein.select(
                    [
                        pl.col("from").alias("id"),
                        pl.col("properties").struct.field("sources").alias("sources"),
                    ]
                ),
            ]
        )
        .unique(subset="id")
        .join(
            onsides_high_confidence.select(
                [
                    "ingredient_id",
                    "rxnorm_name",
                    "rxnorm_term_type",
                    pl.lit(["ONSIDES"]).alias("sources"),
                ]
            ).unique(subset="ingredient_id"),
            left_on="id",
            right_on="ingredient_id",
            how="left",
        )
        .join(drugbank_vocabulary, left_on="id", right_on="drugbank_id", how="left")
        .join(drug_molecule.unnest("metadata"), left_on="id", right_on="id", how="left")
        .join(
            drugcentral_drug.rename({"synonyms": "drugcentral_synonyms"}),
            left_on="id",
            right_on="id",
            how="left",
        )
        .select(
            [
                pl.col("id"),
                pl.lit(Node.DRUG).alias("label"),
                pl.struct(
                    pl.coalesce(
                        [
                            pl.col("rxnorm_name"),
                            pl.col("common_name"),
                            pl.col("name"),
                            pl.col("name_right"),
                        ]
                    ).alias("name"),
                    pl.coalesce(
                        [
                            pl.col("standard_inchi_key"),
                            pl.col("inchi_key"),
                            pl.col("inchi_key_right"),
                        ]
                    ).alias("inchi_key"),
                    pl.coalesce([pl.col("rxnorm_term_type"), pl.col("drug_type")]).alias(
                        "type"
                    ),
                    pl.concat_list(
                        [
                            pl.when(
                                pl.col("drugcentral_synonyms").is_not_null()
                                & (pl.col("drugcentral_synonyms") != "")
                            )
                            .then(pl.col("drugcentral_synonyms").str.split(" | "))
                            .otherwise([]),
                            pl.when(
                                pl.col("synonyms").is_not_null()
                                & (pl.col("synonyms") != "")
                            )
                            .then(pl.col("synonyms").str.split(" | "))
                            .otherwise([]),
                            pl.col("synonyms_right").fill_null([]),
                        ]
                    )
                    .list.eval(pl.element().drop_nulls())
                    .list.unique()
                    .alias(
                        "synonyms"
                    ),  # TODO: this should be a list of strings from the bronze layer
                    pl.col("description"),
                    pl.when(
                        pl.col("accession_numbers").is_not_null()
                        & (pl.col("accession_numbers") != "")
                    )
                    .then(pl.col("accession_numbers").str.split(" | "))
                    .otherwise(None)
                    .alias(
                        "accession_numbers"
                    ),  # TODO: this should be a list of strings from the bronze layer
                    pl.coalesce(pl.col("canonical_smiles"), pl.col("smiles")).alias(
                        "canonical_smiles"
                    ),
                    pl.coalesce(pl.col("cas"), pl.col("cas_reg_no")).alias(
                        "chemical_abstracts_service_number"
                    ),
                    pl.coalesce(pl.col("unii"), pl.col("unii_right")).alias(
                        "unique_ingredient_identifier"
                    ),
                    pl.col("black_box_warning").alias("black_box_warning"),
                    pl.col("year_of_first_approval").alias("year_of_first_approval"),
                    pl.col("maximum_clinical_trial_phase").alias("maximum_clinical_trial_phase"),
                    pl.col("has_been_withdrawn").alias("has_been_withdrawn"),
                    pl.col("is_approved").alias("is_approved"),
                    pl.when(pl.col("trade_names").list.len() > 0)
                    .then(pl.col("trade_names"))
                    .otherwise(None)
                    .alias("trade_names"),
                    pl.coalesce(
                        [
                            pl.col("sources"),
                            pl.col("cross_references").list.eval(
                                pl.element().struct.field("source")
                            ),
                        ]
                    )
                    .list.unique()
                    .alias("sources"),
                    pl.col("cross_references")
                    .list.eval(pl.element().struct.field("ids"))
                    .list.eval(pl.element().explode())
                    .alias("source_ids"),
                    pl.col("cd_id").alias("cd_id"),  # cd = calculated descriptor.
                    pl.col("cd_formula").alias("cd_formula"),
                    pl.col("cd_molweight").alias("cd_mol_weight"),
                    pl.col("clogp").alias("calculated_log_p"),
                    pl.col("alogs").alias("alogs"),  # Predicted aqueous solubility logS
                    pl.col("tpsa"),  # Topological Polar Surface Area
                    pl.col("lipinski"),
                    pl.col("no_formulations").alias("number_of_formulations"),
                    pl.col("molfile_base64").alias("mol_file_base64"),
                    pl.col("molimg_base64").alias("mol_image_base64"),
                    pl.col("mrdef"),
                    pl.col("enhanced_stereo").alias("enhanced_stereo"),
                    pl.col("arom_c").alias("aromatic_carbons"),
                    pl.col("sp3_c").alias("sp3_count"),
                    pl.col("sp2_c").alias("sp2_count"),
                    pl.col("sp_c").alias("sp_count"),
                    pl.col("halogen").alias("halogen_count"),  # Count of halogen atoms
                    pl.col("hetero_sp2_c").alias(
                        "hetero_sp2_count"
                    ),  # Count of sp2-hybridized hetero atoms
                    pl.col("rotb").alias("rotatable_bonds"),
                    pl.col("o_n").alias("o_n"),  # Count of oxygen + nitrogen atoms
                    pl.col("oh_nh").alias("oh_nh"),  # Count of OH/NH groups
                    pl.col("inchi"),
                    pl.col("rgb"),
                    pl.col("fda_labels").alias("fda_labels"),
                    pl.col("status"),
                    pl.col("struct_id").alias(
                        "struct_id"
                    ),  # Primary key of the chemical structure in DrugCentral's structures table
                ).alias("properties"),
            ]
        )
    ).sort(by="id")

    # TODO: there are 2,699 drugs that only contains the column id but no properties.
    # This is because they are defined as "parent" relationships in the drug_drug dataset, but not present
    # in the drug_molecule dataset. You can verify this by filtering for: filter(pl.col("name").is_null())


drug_node = node(
    run,
    inputs={
        "onsides_high_confidence": "bronze.onsides.high_confidence",
        "drugbank_vocabulary": "bronze.drugbank.vocabulary",
        "drug_molecule": "bronze.opentargets.drug_molecule",
        "drugcentral_drug": "bronze.drugcentral.drug",
        "drug_drug": "silver.edges.drug_drug",
        "drug_protein": "silver.edges.drug_protein",
        "drug_disease": "silver.edges.drug_disease",
        "drug_phenotype": "silver.edges.drug_phenotype",
    },
    outputs="nodes.drug",
    name="drug",
    tags=["silver"],
)
