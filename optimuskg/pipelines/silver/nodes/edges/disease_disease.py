import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node, Relation, Source


def run(
    disease: pl.DataFrame,
) -> pl.DataFrame:
    return (
        disease.with_columns(
            pl.col("metadata").struct.field("parents"),
        )
        .explode("parents")
        .filter(
            pl.col("parents").is_not_null(),
            ~pl.col("id").str.contains("HP"),
            ~pl.col("id").str.contains("Orphanet"),  # TODO: why filter for Orphanet?
        )
        .with_columns(
            pl.col("parents").alias("from"),
            pl.col("id").alias("to"),
            pl.lit(Edge.format_label(Node.DISEASE, Node.DISEASE)).alias("label"),
            pl.lit(Relation.PARENT).alias("relation"),
            pl.lit(False).alias("undirected"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit([Source.OPEN_TARGETS])
                            .cast(pl.List(pl.String))
                            .alias("direct"),
                            pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                        ]
                    ).alias("sources"),
                ]
            ).alias("properties"),
        )
        .select(["from", "to", "label", "relation", "undirected", "properties"])
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


disease_disease_node = node(
    run,
    inputs={
        "disease": "bronze.opentargets.disease",
    },
    outputs="edges.disease_disease",
    name="disease_disease",
    tags=["silver"],
)
