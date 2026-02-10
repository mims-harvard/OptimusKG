import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node, Relation


def run(
    reactome_relations: pl.DataFrame,
) -> pl.DataFrame:
    return (
        reactome_relations.select(
            pl.col("reactome_id_1").alias("from"),
            pl.col("reactome_id_2").alias("to"),
            pl.lit(Edge.format_label(Node.PATHWAY, Node.PATHWAY)).alias("label"),
            pl.lit(Relation.PARENT).alias("relation"),
            pl.lit(False).alias("undirected"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit(["REACTOME"]).alias("direct"),
                            pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                        ]
                    ).alias("sources"),
                ]
            ).alias("properties"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


pathway_pathway_node = node(
    run,
    inputs={
        "reactome_relations": "bronze.reactome.reactome_relations",
    },
    outputs="edges.pathway_pathway",
    name="pathway_pathway",
    tags=["silver"],
)
