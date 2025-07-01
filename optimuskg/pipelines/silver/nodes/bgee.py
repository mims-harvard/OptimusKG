import polars as pl
from kedro.pipeline import node


def run(
    gene_expressions_in_anatomy: pl.DataFrame,
) -> pl.DataFrame:
    df = (
        gene_expressions_in_anatomy.filter(
            pl.col("expression").is_in(["present", "absent"])
        )
        .with_columns(
            [
                pl.when(pl.col("expression") == "present")
                .then(pl.lit("expression present"))
                .otherwise(pl.lit("expression absent"))
                .alias("relation_type"),
                pl.col("gene_id").alias("x_id"),
                pl.lit("gene").alias("x_type"),
                pl.col("gene_name").alias("x_name"),
                pl.lit("BGEE").alias("x_source"),
                pl.col("anatomy_id").alias("y_id"),
                pl.lit("anatomy").alias("y_type"),
                pl.col("anatomy_name").alias("y_name"),
                pl.lit("BGEE").alias("y_source"),
                pl.lit("anatomy_protein").alias("relation"),
            ]
        )
        .drop(["expression", "gene_id", "gene_name", "anatomy_id", "anatomy_name"])
    )

    df = df.select(
        [
            "call_quality",
            "expression_rank",
            "relation",
            "relation_type",
            "x_id",
            "x_type",
            "x_name",
            "x_source",
            "y_id",
            "y_type",
            "y_name",
            "y_source",
        ]
    )

    return df


bgee_node = node(
    run,
    inputs={"gene_expressions_in_anatomy": "bronze.bgee.gene_expressions_in_anatomy"},
    outputs="bgee.anatomy_protein",
    name="bgee",
    tags=["silver"],
)
