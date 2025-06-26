import polars as pl
from kedro.pipeline import node


def process_disease_phenotype(
    hpo_mappings: pl.DataFrame,
    mondo_xrefs: pl.DataFrame,
    mondo_terms: pl.DataFrame,
    phenotypes: pl.DataFrame,
) -> pl.DataFrame:
    disease_phenotype = (
        hpo_mappings.filter(
            pl.col("database_id").str.contains(
                "Orphanet_"
            )  # NOTE: Only keep diseases from the phenotype mappings
        )
        .with_columns(
            pl.when(pl.col("qualifier").is_null())
            .then(pl.lit("phenotype_present"))
            .otherwise(pl.lit("phenotype_absent"))
            .alias("relation_type"),
            pl.col("database_id").alias("disease_id"),
        )
        .drop(["qualifier", "database_id"])
    )

    disease_phenotype = (
        disease_phenotype.join(
            mondo_xrefs,
            left_on="disease_id",
            right_on="xref_id",
            how="left",
        )
        .join(
            phenotypes,
            left_on="hpo_id",
            right_on="id",
            how="left",
        )
        .join(
            mondo_terms,
            left_on="id",
            right_on="id",
            how="left",
        )
    )

    disease_phenotype = disease_phenotype.rename(
        {"id": "x_id", "disease_name": "x_name", "hpo_id": "y_id", "name": "y_name"}
    )

    disease_phenotype = disease_phenotype.with_columns(
        pl.lit("disease").alias("x_type"),
        pl.lit("MONDO").alias("x_source"),
        pl.lit("phenotype").alias("y_type"),
        pl.lit("HPO").alias("y_source"),
        pl.lit("disease_phenotype").alias("relation"),
        pl.col("relation_type").alias("relation_type"),
    )

    disease_phenotype = disease_phenotype.select(
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

    return disease_phenotype


disease_phenotype_node = node(
    process_disease_phenotype,
    inputs={
        "hpo_mappings": "bronze.ontology.hpo_mappings",
        "mondo_xrefs": "bronze.ontology.mondo_xrefs",
        "mondo_terms": "bronze.ontology.mondo_terms",
        "phenotypes": "bronze.ontology.phenotypes",
    },
    outputs="ontology.disease_phenotype",
    name="disease_phenotype",
    tags=["silver"],
)
