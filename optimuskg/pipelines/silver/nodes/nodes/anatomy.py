import polars as pl
from kedro.pipeline import node


def run(
    anatomy_protein: pl.DataFrame,
    anatomy_anatomy: pl.DataFrame,
    uberon_terms: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                anatomy_anatomy.select(
                    pl.concat_list(["from", "to"]).explode().alias("id")
                ),
                anatomy_protein.select(pl.col("from").alias("id")),
            ]
        )
        .unique(subset="id")
        .join(uberon_terms, left_on="id", right_on="id", how="inner")
        .select(
            pl.col("id"),
            pl.lit("anatomy").alias("label"),
            pl.struct(
                [
                    pl.lit("UBERON").alias("source"),
                    pl.col("name"),
                    pl.col("definition"),
                    pl.col("xrefs"),  # TODO: cast to a proper list
                    pl.col("synonyms"),  # TODO: cast to a proper list
                    pl.col("ontology_description").alias("ontology_description"),
                    pl.col("ontology_title").alias("ontology_title"),
                    pl.col("ontology_license").alias("ontology_license"),
                    pl.col("ontology_version").alias("ontology_version"),
                ]
            ).alias("properties"),
        )
        .sort(by="id")
    )


anatomy_node = node(
    run,
    inputs={
        "anatomy_protein": "silver.edges.anatomy_protein",
        "anatomy_anatomy": "silver.edges.anatomy_anatomy",
        "uberon_terms": "bronze.ontology.uberon_terms",
    },
    outputs="nodes.anatomy",
    name="anatomy",
    tags=["silver"],
)
