import polars as pl
from kedro.pipeline import node


def run(
    protein_biological_process_interactions: pl.DataFrame,
    exposure_biological_process: pl.DataFrame,
    biological_process_biological_process: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                protein_biological_process_interactions.filter(
                    pl.col("y_type") == "biological_process"
                ).select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                exposure_biological_process.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                biological_process_biological_process.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                biological_process_biological_process.select(
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


biological_process_node = node(
    run,
    inputs={
        "protein_biological_process_interactions": "silver.ncbigene.protein_biological_process_interactions",
        "exposure_biological_process": "silver.ctd.ctd_exposure_biological_process_interactions",
        "biological_process_biological_process": "silver.ontology.biological_process_biological_process_interactions",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="nodes.biological_process",
    name="biological_process",
    tags=["gold"],
)
