import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node


def run(
    go_relations: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    biological_process = go_terms.filter(pl.col("type") == "biological_process")

    return (
        go_relations.filter(
            pl.col("edge_type") == "is_a"
        )  # TODO: keep all relations (not just is_a)
        .join(
            biological_process.select("id"), left_on="tail", right_on="id", how="inner"
        )
        .join(
            biological_process.select("id"), left_on="head", right_on="id", how="inner"
        )
        .select(
            [
                # NOTE: go_relations head-tail columns are for "is_a" (i.e. child) relations but we represent the inverse (i.e. parent) relations.
                pl.col("tail").alias("from"),
                pl.col("head").alias("to"),
                pl.lit(
                    Edge.format_label(Node.BIOLOGICAL_PROCESS, Node.BIOLOGICAL_PROCESS)
                ).alias("label"),
                pl.col("edge_type").alias("relation"),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.lit(["GO"]).alias("sources"),
                    ]
                ).alias("properties"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


biological_process_biological_process_node = node(
    run,
    inputs={
        "go_relations": "bronze.ontology.go_relations",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="edges.biological_process_biological_process",
    name="biological_process_biological_process",
    tags=["silver"],
)
