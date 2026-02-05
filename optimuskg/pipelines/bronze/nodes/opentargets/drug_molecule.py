import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    drug_molecule: pl.DataFrame,
) -> pl.DataFrame:
    return (
        drug_molecule.select(
            pl.col("id"),
            pl.col("name"),
            pl.struct(
                pl.col("canonicalSmiles").alias("canonical_smiles"),
                pl.col("inchiKey").alias("inchi_key"),
                pl.col("drugType").alias("drug_type"),
                pl.col("blackBoxWarning").alias("black_box_warning"),
                pl.col("yearOfFirstApproval").alias("year_of_first_approval"),
                pl.col("maximumClinicalTrialPhase").alias(
                    "maximum_clinical_trial_phase"
                ),
                pl.col("parentId").alias("parent_id"),
                pl.col("hasBeenWithdrawn").alias("has_been_withdrawn"),
                pl.col("isApproved").alias("is_approved"),
                pl.col("tradeNames").alias("trade_names"),
                pl.col("synonyms"),
                pl.col("crossReferences").alias("cross_references"),
                pl.col("childChemblIds").alias("child_chembl_ids"),
                pl.col("linkedDiseases").alias("linked_diseases"),
                pl.col("linkedTargets").alias("linked_targets"),
                pl.col("description"),
            ).alias("metadata"),
        )
        .unique(subset=["id", "name"])
        .sort(by=["id", "name"])
    )


drug_molecule_node = node(
    run,
    inputs={
        "drug_molecule": "landing.opentargets.drug_molecule",
    },
    outputs="opentargets.drug_molecule",
    name="drug_molecule",
    tags=["bronze"],
)
