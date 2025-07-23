import polars as pl
from kedro.pipeline import node


def run(
    protein_cellular_component: pl.DataFrame,
    exposure_cellular_component: pl.DataFrame,
    cellular_component_cellular_component: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                protein_cellular_component.filter(
                    pl.col("y_type") == "cellular_component"
                ).select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                exposure_cellular_component.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                cellular_component_cellular_component.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                cellular_component_cellular_component.select(
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


cellular_component_node = node(
    run,
    inputs={
        "protein_cellular_component": "silver.ncbigene.protein_cellular_component",
        "exposure_cellular_component": "silver.ctd.exposure_cellular_component",
        "cellular_component_cellular_component": "silver.ontology.cellular_component_cellular_component",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="nodes.cellular_component",
    name="cellular_component",
    tags=["gold"],
)
