import polars as pl
from kedro.pipeline import node


def process_molecular_function_nodes(
    protein_molecular_function_interactions: pl.DataFrame,
) -> pl.DataFrame:
    return (
        protein_molecular_function_interactions.filter(
            pl.col("y_type") == "molecular_function"
        ).select(
            pl.col("y_id").alias("id"),
            pl.col("y_type").alias("type"),
            pl.col("y_name").alias("name"),
            pl.col("y_source").alias("source"),
        )
    ).unique()


biological_process_node = node(
    process_molecular_function_nodes,
    inputs={
        "protein_molecular_function_interactions": "silver.ncbigene.protein_molecular_function_interactions",
    },
    outputs="nodes.molecular_function",
    name="molecular_function",
    tags=["gold"],
)
