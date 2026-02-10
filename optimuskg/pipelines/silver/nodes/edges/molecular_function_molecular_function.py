import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node, Relation, Source


def run(
    go_relations: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    molecular_function = go_terms.filter(pl.col("type") == "molecular_function")

    return (
        go_relations.filter(
            pl.col("edge_type") == "is_a"
        )  # TODO: keep all relations (not just is_a)
        .join(
            molecular_function.select("id"), left_on="tail", right_on="id", how="inner"
        )
        .join(
            molecular_function.select("id"), left_on="head", right_on="id", how="inner"
        )
        .select(
            [
                # NOTE: go_relations head-tail columns are for "is_a" (i.e. child) relations but we represent the inverse (i.e. parent) relations.
                pl.col("tail").alias("from"),
                pl.col("head").alias("to"),
                pl.lit(
                    Edge.format_label(Node.MOLECULAR_FUNCTION, Node.MOLECULAR_FUNCTION)
                ).alias("label"),
                pl.lit(Relation.IS_A).alias("relation"),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    pl.struct(
                        pl.lit([Source.GO]).cast(pl.List(pl.String)).alias("direct"),
                        pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                    ).alias("sources"),
                ).alias("properties"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


molecular_function_molecular_function_node = node(
    run,
    inputs={
        "go_relations": "bronze.ontology.go_relations",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="edges.molecular_function_molecular_function",
    name="molecular_function_molecular_function",
    tags=["silver"],
)
