import polars as pl
from kedro.pipeline import node


def run(  # noqa: PLR0913
    opentargets_edges: pl.DataFrame,
    disgenet_effect_protein: pl.DataFrame,
    disease_phenotype: pl.DataFrame,
    phenotype_phenotype: pl.DataFrame,
    drug_phenotype: pl.DataFrame,
    hp_terms: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                opentargets_edges.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                opentargets_edges.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                disgenet_effect_protein.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                disease_phenotype.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                phenotype_phenotype.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                phenotype_phenotype.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                drug_phenotype.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
            ],
            how="vertical",
        )
        .filter(pl.col("type") == "phenotype")
        .unique()
        .join(
            hp_terms,
            left_on="id",
            right_on="id",
            how="left",
        )
        .select(
            "id",
            "name",
            "type",
            "source",
            "xrefs",
            "synonyms",
            "ontology_description",
            "ontology_title",
            "ontology_license",
            "ontology_version",
        )
    )


phenotype_node = node(
    run,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "disgenet_effect_protein": "silver.disgenet.effect_protein",
        "disease_phenotype": "silver.ontology.disease_phenotype",
        "phenotype_phenotype": "silver.ontology.phenotype_phenotype",
        "drug_phenotype": "silver.onsides.drug_phenotype",
        "hp_terms": "bronze.ontology.hp_terms",
    },
    outputs="nodes.phenotype",
    name="phenotype",
    tags=["gold"],
)
