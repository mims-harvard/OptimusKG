import polars as pl
from kedro.pipeline import node


def run(
    anatomy_protein: pl.DataFrame,
    anatomy_anatomy: pl.DataFrame,
    uberon_terms: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                anatomy_protein.filter(pl.col("y_type") == "anatomy").select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                anatomy_anatomy.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                anatomy_anatomy.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
            ]
        )
        .join(
            uberon_terms,
            left_on="id",
            right_on="id",
            how="left",
        )
        .unique()
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


anatomy_node = node(
    run,
    inputs={
        "anatomy_protein": "silver.bgee.anatomy_protein",
        "anatomy_anatomy": "silver.ontology.anatomy_anatomy",
        "uberon_terms": "bronze.ontology.uberon_terms",
    },
    outputs="nodes.anatomy",
    name="anatomy",
    tags=["gold"],
)
