import polars as pl
from kedro.pipeline import node


def run(
    uberon_terms: pl.DataFrame,
    uberon_relations: pl.DataFrame,
) -> pl.DataFrame:
    df = uberon_relations.join(
        uberon_terms.select(["id", "name"]), left_on="id", right_on="id", how="left"
    )

    df = df.rename({"id": "x_id", "name": "x_name"})

    df = df.join(
        uberon_terms.select(["id", "name"]),
        left_on="relation_id",
        right_on="id",
        how="left",
    ).rename({"relation_id": "y_id", "name": "y_name"})

    df = df.with_columns(
        [
            pl.lit("anatomy").alias("x_type"),
            pl.lit("UBERON").alias("x_source"),
            pl.lit("anatomy").alias("y_type"),
            pl.lit("UBERON").alias("y_source"),
            pl.lit("anatomy_anatomy").alias("relation"),
            pl.lit("parent-child").alias("relation_type"),
        ]
    )

    return df.select(
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


anatomy_anatomy_node = node(
    run,
    inputs={
        "uberon_terms": "bronze.ontology.uberon_terms",
        "uberon_relations": "bronze.ontology.uberon_relations",
    },
    outputs="anatomy_anatomy",
    name="anatomy_anatomy",
    tags=["silver"],
)
