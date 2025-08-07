import polars as pl
from kedro.pipeline import node


def run(
    go_relations: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    cellular_component = go_terms.filter(pl.col("type") == "cellular_component")

    return (
        go_relations.filter(
            pl.col("edge_type") == "is_a"
        )  # TODO: keep all relations (not just is_a)
        .join(
            cellular_component.select("id"), left_on="tail", right_on="id", how="inner"
        )
        .join(
            cellular_component.select("id"), left_on="head", right_on="id", how="inner"
        )
        .select(
            [
                # NOTE: go_relations head-tail columns are for "is_a" (i.e. child) relations but we represent the inverse (i.e. parent) relations.
                pl.col("tail").alias("from"),
                pl.col("head").alias("to"),
                pl.lit("cellular_component_cellular_component").alias("relation"),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.col("edge_type").alias("relationType"),
                        pl.lit(["GO"]).alias("sources"),
                    ]
                ).alias("properties"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


cellular_component_cellular_component_node = node(
    run,
    inputs={
        "go_relations": "bronze.ontology.go_relations",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="edges.cellular_component_cellular_component",
    name="cellular_component_cellular_component",
    tags=["silver"],
)
