import polars as pl
from kedro.pipeline import node


def run(  # noqa: PLR0913
    opentargets_edges: pl.DataFrame,
    disease_disease: pl.DataFrame,
    exposure_disease: pl.DataFrame,
    drug_disease: pl.DataFrame,
    disease_protein: pl.DataFrame,
    disease_phenotype: pl.DataFrame,
    mondo_terms: pl.DataFrame,
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
                disease_disease.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                disease_disease.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                exposure_disease.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                drug_disease.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                disease_protein.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                disease_phenotype.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
            ],
            how="vertical",
        )
        .filter(pl.col("type") == "disease")
        .unique()
        .join(
            mondo_terms,
            left_on="id",
            right_on="id",
            how="left",
        )
        .select(
            "id",
            "type",
            "name",
            "source",
            "definition",
            "xrefs",
            "synonyms",
            "ontology_description",
            "ontology_title",
            "ontology_license",
            "ontology_version",
        )
    )


disease_node = node(
    run,
    inputs={
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "disease_disease": "silver.disease_disease",
        "exposure_disease": "silver.exposure_disease",
        "drug_disease": "silver.drug_disease",
        "disease_protein": "silver.disease_protein",
        "disease_phenotype": "silver.disease_phenotype",
        "mondo_terms": "bronze.ontology.mondo_terms",
    },
    outputs="nodes.disease",
    name="disease",
    tags=["gold"],
)
