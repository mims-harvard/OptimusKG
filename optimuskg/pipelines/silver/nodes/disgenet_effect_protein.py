import polars as pl
from kedro.pipeline import node


def process_disgenet_effect_protein(
    disgenet_phenotypes: pl.DataFrame,
    phenotypes: pl.DataFrame,
    phenotypes_xrefs: pl.DataFrame,
) -> pl.DataFrame:
    df_prot_phe = disgenet_phenotypes.join(
        phenotypes_xrefs, left_on="disease_id", right_on="ontology_id", how="inner"
    )
    df_prot_phe = df_prot_phe.join(
        phenotypes, left_on="hp_id", right_on="id", how="left"
    )

    df_prot_phe = df_prot_phe.rename(
        {"gene_id": "x_id", "gene_symbol": "x_name", "hp_id": "y_id", "name": "y_name"}
    )

    df_prot_phe = df_prot_phe.with_columns(
        [
            pl.lit("gene").alias("x_type"),
            pl.lit("NCBI").alias("x_source"),
            pl.lit("phenotype").alias("y_type"),
            pl.lit("HPO").alias("y_source"),
            pl.lit("phenotype_protein").alias("relation"),
            pl.lit("associated with").alias("relation_type"),
        ]
    )

    df_prot_phe = df_prot_phe.select(
        [
            "relation",
            "relation_type",
            "x_id",
            "x_type",
            "x_name",
            "x_source",
            "y_id",
            "y_type",
            "y_name",
            "y_source",
        ]
    )

    return df_prot_phe


disgenet_effect_protein_node = node(
    process_disgenet_effect_protein,
    inputs={
        "disgenet_phenotypes": "bronze.disgenet.disgenet_phenotypes",
        "phenotypes": "bronze.opentargets.phenotypes",
        "phenotypes_xrefs": "bronze.opentargets.phenotypes_xrefs",
    },
    outputs="disgenet.effect_protein",
    name="disgenet_effect_protein",
    tags=["silver"],
)
