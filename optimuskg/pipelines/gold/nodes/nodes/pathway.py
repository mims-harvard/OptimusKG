import polars as pl
from kedro.pipeline import node


def process_pathway_nodes(  # noqa: PLR0913
    pathway_pathway_interactions: pl.DataFrame,
    pathway_protein_interactions: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                pathway_pathway_interactions.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                pathway_pathway_interactions.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                pathway_protein_interactions.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
            ],
            how="vertical",
        )
        .filter(pl.col("type") == "pathway")
        .unique()
    )


pathway_node = node(
    process_pathway_nodes,
    inputs={
        "pathway_pathway_interactions": "silver.reactome.pathway_pathway_interactions",
        "pathway_protein_interactions": "silver.reactome.pathway_protein_interactions",
    },
    outputs="nodes.pathway",
    name="pathway",
    tags=["gold"],
)
