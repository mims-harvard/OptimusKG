import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.utils import clean_edges


def process_bgee(
    gene_expressions_in_anatomy: pl.DataFrame,
) -> pl.DataFrame:
    df = gene_expressions_in_anatomy.filter(
        pl.col("expression").is_in(["present", "absent"])
    ).with_columns(
        [
            pl.when(pl.col("expression") == "present")
            .then(pl.lit("anatomy_protein_present"))
            .otherwise(pl.lit("anatomy_protein_absent"))
            .alias("relation"),
            pl.when(pl.col("expression") == "present")
            .then(pl.lit("expression present"))
            .otherwise(pl.lit("expression absent"))
            .alias("display_relation"),
            pl.col("gene_id").alias("x_id"),
            pl.lit("gene/protein").alias("x_type"),
            pl.col("gene_name").alias("x_name"),
            pl.lit("BGEE").alias("x_source"),
            pl.col("anatomy_id").alias("y_id"),
            pl.lit("anatomy").alias("y_type"),
            pl.col("anatomy_name").alias("y_name"),
            pl.lit("BGEE").alias("y_source"),
        ]
    )

    df = clean_edges(df)

    return df


bgee_node = node(
    process_bgee,
    inputs={"gene_expressions_in_anatomy": "bronze.bgee.gene_expressions_in_anatomy"},
    outputs="bgee.gene_expressions_in_anatomy",
    name="bgee",
    tags=["silver"],
)
