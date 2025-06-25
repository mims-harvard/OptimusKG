import polars as pl
from kedro.pipeline import node


def process_molecular_function_nodes(
    protein_molecular_function_interactions: pl.DataFrame,
    exposure_molecular_function: pl.DataFrame,
    molecular_function_molecular_function: pl.DataFrame,
) -> pl.DataFrame:
    return pl.concat(
        [
            protein_molecular_function_interactions.filter(
                pl.col("y_type") == "molecular_function"
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
    ).unique()


molecular_function_node = node(
    process_molecular_function_nodes,
    inputs={
        "protein_molecular_function_interactions": "silver.ncbigene.protein_molecular_function_interactions",
        "exposure_molecular_function": "silver.ctd.ctd_exposure_molecular_function_interactions",
        "molecular_function_molecular_function": "silver.ontology.molecular_function_molecular_function_interactions",
    },
    outputs="nodes.molecular_function",
    name="molecular_function",
    tags=["gold"],
)
