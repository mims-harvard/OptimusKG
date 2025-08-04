import polars as pl
from kedro.pipeline import node


def run(
    gene_expressions_in_anatomy: pl.DataFrame,
) -> pl.DataFrame:
    return (
        gene_expressions_in_anatomy.filter(
            pl.col("expression").is_in(["present", "absent"])
        )
        .select(
            pl.col("anatomy_id").alias("from"),
            pl.col("gene_id").alias("to"),
            pl.lit("anatomy_protein").alias("relation"),
            pl.lit(True).alias("undirected"),
            pl.struct(
                [
                    pl.when(pl.col("expression") == "present")
                    .then(pl.lit("expression present"))
                    .otherwise(pl.lit("expression absent"))
                    .alias("relationType"),
                    pl.col("expression_rank").alias("expressionRank"),
                    pl.col("call_quality").alias("callQuality"),
                    pl.lit(["BGEE"]).alias("sources"),
                ]
            ).alias("properties"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


anatomy_protein_node = node(
    run,
    inputs={"gene_expressions_in_anatomy": "bronze.bgee.gene_expressions_in_anatomy"},
    outputs="anatomy_protein",
    name="anatomy_protein",
    tags=["silver"],
)
