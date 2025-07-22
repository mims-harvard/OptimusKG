import polars as pl
from kedro.pipeline import node


def run(
    go_relations: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    cc = go_terms.filter(pl.col("type") == "cellular_component")

    df_go_edges = go_relations.rename({"tail": "x_id", "head": "y_id"})

    return (
        df_go_edges.join(
            cc.select(["id", "name"]), left_on="x_id", right_on="id", how="inner"
        )
        .rename({"name": "x_name"})
        .join(cc.select(["id", "name"]), left_on="y_id", right_on="id", how="inner")
        .rename({"name": "y_name"})
        .with_columns(
            [
                pl.lit("cellular_component").alias("x_type"),
                pl.lit("GO").alias("x_source"),
                pl.lit("cellular_component").alias("y_type"),
                pl.lit("GO").alias("y_source"),
                pl.lit("cellular_component_cellular_component").alias("relation"),
                pl.lit("parent-child").alias("relation_type"),
            ]
        )
        .with_columns(
            pl.concat_list([pl.col("x_id"), pl.col("y_id")])
            .list.sort()
            .alias("sorted_ids")
        )
        .unique("sorted_ids", keep="first")
        .drop("sorted_ids")
        .select(
            [
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
    )


cellular_component_cellular_component_node = node(
    run,
    inputs={
        "go_relations": "bronze.ontology.go_relations",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="ontology.cellular_component_cellular_component",
    name="cellular_component_cellular_component",
    tags=["silver"],
)
