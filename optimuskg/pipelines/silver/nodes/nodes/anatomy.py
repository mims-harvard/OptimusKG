import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Node


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
            pl.lit(Node.ANATOMY).alias("label"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit(["UBERON"]).alias("direct"),
                            pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                        ]
                    ).alias("sources"),
                    pl.col("name"),
                    pl.col("definition"),
                    pl.col("xrefs"),
                    pl.col("synonyms"),
                    pl.col("ontology"),
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
