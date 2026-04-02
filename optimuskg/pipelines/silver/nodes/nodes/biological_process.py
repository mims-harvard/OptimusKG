import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Node, Source


def run(
    biological_process_gene: pl.DataFrame,
    exposure_biological_process: pl.DataFrame,
    biological_process_biological_process: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                biological_process_gene.select(pl.col("from").alias("id")),
                exposure_biological_process.select(pl.col("to").alias("id")),
                biological_process_biological_process.select(
                    pl.concat_list(["from", "to"]).explode().alias("id")
                ),
            ]
        )
        .unique(subset="id")
        .join(
            go_terms, left_on="id", right_on="id", how="left"
        )  # TODO: there are 3 ids that are not in the go_terms table
        .select(
            pl.col("id"),
            pl.lit(Node.BIOLOGICAL_PROCESS).alias("label"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit([Source.GO])
                            .cast(pl.List(pl.String))
                            .alias("direct"),
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


biological_process_node = node(
    run,
    inputs={
        "biological_process_gene": "silver.edges.biological_process_gene",
        "exposure_biological_process": "silver.edges.exposure_biological_process",
        "biological_process_biological_process": "silver.edges.biological_process_biological_process",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="nodes.biological_process",
    name="biological_process",
    tags=["silver"],
)
