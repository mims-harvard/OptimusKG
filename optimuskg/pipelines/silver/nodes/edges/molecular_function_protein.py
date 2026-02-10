import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node, Relation, Source


def run(
    target: pl.DataFrame,
) -> pl.DataFrame:
    return (
        target.select(
            [pl.col("id").alias("target_id"), pl.col("metadata").struct.field("go")]
        )
        .explode("go")
        .unnest("go")
        .filter(pl.col("id").is_not_null())
        .group_by(["target_id", "id"])
        .agg(
            [
                pl.lit(Edge.format_label(Node.MOLECULAR_FUNCTION, Node.PROTEIN)).alias(
                    "label"
                ),
                pl.lit(True).alias("undirected"),
                pl.col("source").unique().alias("indirect_sources"),
                pl.col("evidence").unique(),
                pl.col("aspect").unique().alias("aspect"),
                pl.col("gene_product").unique(),
                pl.col("eco_id").drop_nulls().unique().alias("eco_ids"),
            ]
        )
        .filter(
            pl.col("aspect") == ["F"]  # F = molecular function
        )
        .select(
            [
                pl.col("id")
                .str.replace("GO:", "GO_")
                .alias("from"),  # use _ to match biolink mapping
                pl.col("target_id").alias("to"),
                pl.col("label"),
                pl.lit(Relation.INTERACTS_WITH).alias("relation"),
                pl.col("undirected"),
                pl.struct(
                    [
                        pl.struct(
                            [
                                pl.lit([Source.OPEN_TARGETS])
                                .cast(pl.List(pl.String))
                                .alias("direct"),
                                pl.col("indirect_sources").alias("indirect"),
                            ]
                        ).alias("sources"),
                        pl.col("evidence"),
                        pl.col("gene_product"),
                        pl.col("eco_ids"),
                    ]
                ).alias("properties"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


molecular_function_protein_node = node(
    run,
    inputs={
        "target": "bronze.opentargets.target",
    },
    outputs="edges.molecular_function_protein",
    name="molecular_function_protein",
    tags=["silver"],
)
