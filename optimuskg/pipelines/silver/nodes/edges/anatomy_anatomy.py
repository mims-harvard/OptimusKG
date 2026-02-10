import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node, Relation, Source


def run(
    uberon_terms: pl.DataFrame,
    uberon_relations: pl.DataFrame,
) -> pl.DataFrame:
    return (
        uberon_relations.join(
            uberon_terms.select(["id"]), left_on="id", right_on="id", how="left"
        )
        .rename({"id": "x_id"})
        .join(
            uberon_terms.select(["id"]),
            left_on="relation_id",
            right_on="id",
            how="left",
        )
        .rename({"relation_id": "y_id"})
        .select(
            pl.col("y_id").alias("from"),
            pl.col("x_id").alias("to"),
            pl.lit(Edge.format_label(Node.ANATOMY, Node.ANATOMY)).alias("label"),
            pl.lit(Relation.PARENT).alias("relation"),
            pl.lit(False).alias("undirected"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit([Source.UBERON])
                            .cast(pl.List(pl.String))
                            .alias("direct"),
                            pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                        ]
                    ).alias("sources"),
                ]
            ).alias("properties"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


anatomy_anatomy_node = node(
    run,
    inputs={
        "uberon_terms": "bronze.ontology.uberon_terms",
        "uberon_relations": "bronze.ontology.uberon_relations",
    },
    outputs="edges.anatomy_anatomy",
    name="anatomy_anatomy",
    tags=["silver"],
)
