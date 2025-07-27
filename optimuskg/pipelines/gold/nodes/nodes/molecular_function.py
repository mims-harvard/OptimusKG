import polars as pl
from kedro.pipeline import node


def run(
    protein_molecular_function: pl.DataFrame,
    exposure_molecular_function: pl.DataFrame,
    molecular_function_molecular_function: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                protein_molecular_function.filter(
                    pl.col("y_type")
                    == "molecular_function"  # TODO: Why this filter is needed?
                ).select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                exposure_molecular_function.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                molecular_function_molecular_function.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                molecular_function_molecular_function.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
            ]
        )
        .unique()
        .join(
            go_terms,
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


molecular_function_node = node(
    run,
    inputs={
        "protein_molecular_function": "silver.protein_molecular_function",
        "exposure_molecular_function": "silver.exposure_molecular_function",
        "molecular_function_molecular_function": "silver.molecular_function_molecular_function",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="nodes.molecular_function",
    name="molecular_function",
    tags=["gold"],
)
