import polars as pl
from kedro.pipeline import node


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
            pl.lit("anatomy_anatomy").alias("relation"),
            pl.lit(False).alias("undirected"),
            pl.struct(
                [
                    pl.lit("parent").alias("relationType"),
                    pl.lit(["UBERON"]).alias("sources"),
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
