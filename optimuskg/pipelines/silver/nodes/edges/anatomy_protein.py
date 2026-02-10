import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node, Relation


def run(
    gene_expressions_in_anatomy: pl.DataFrame,
) -> pl.DataFrame:
    return (
        gene_expressions_in_anatomy.filter(
            pl.col("expression").is_in(["present", "absent"])
        )
        .select(
            pl.col("anatomy_id")
            .str.replace("UBERON:", "UBERON_")
            .alias("from"),  # NOTE: using _ to match biolink mapping
            pl.col("gene_id").alias("to"),
            pl.lit(Edge.format_label(Node.ANATOMY, Node.PROTEIN)).alias("label"),
            pl.when(pl.col("expression") == "present")
            .then(pl.lit(Relation.EXPRESSION_PRESENT))
            .otherwise(pl.lit(Relation.EXPRESSION_ABSENT))
            .alias("relation"),
            pl.lit(True).alias("undirected"),
            pl.struct(
                [  # TODO: add more metadata with more columns from the landing version of gene_expressions_in_anatomy
                    pl.col("expression_rank")
                    .cast(pl.Float64)
                    .cast(pl.Int32)
                    .alias("expression_rank"),
                    pl.col("call_quality").alias("call_quality"),
                    pl.lit(["BGEE"]).alias("direct_sources"),
                    pl.lit([]).cast(pl.List(pl.String)).alias("indirect_sources"),
                ]
            ).alias("properties"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


anatomy_protein_node = node(
    run,
    inputs={"gene_expressions_in_anatomy": "bronze.bgee.gene_expressions_in_anatomy"},
    outputs="edges.anatomy_protein",
    name="anatomy_protein",
    tags=["silver"],
)
