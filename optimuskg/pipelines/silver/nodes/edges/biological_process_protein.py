import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Node, Edge


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
                pl.lit(Edge.format_label(Node.BIOLOGICAL_PROCESS, Node.PROTEIN)).alias("label"),
                pl.lit(True).alias("undirected"),
                pl.col("source").drop_nulls().unique().alias("sources"),
                pl.col("evidence").drop_nulls().unique(),
                pl.col("aspect").drop_nulls().unique().alias("aspect"),
                pl.col("gene_product").drop_nulls().unique(),
                pl.col("eco_id").drop_nulls().unique().alias("eco_ids"),
            ]
        )
        .filter(pl.col("aspect") == ["P"])
        .select(  # P = biological process
            [
                pl.col("id")
                .str.replace("GO:", "GO_")
                .alias("from"),  # use _ to match biolink mapping
                pl.col("target_id").alias("to"),
                pl.col("label"),
                pl.lit("interacts with").alias("relation"),
                pl.col("undirected"),
                pl.struct(
                    [
                        pl.col("sources"),
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


biological_process_protein_node = node(
    run,
    inputs={
        "target": "bronze.opentargets.target",
    },
    outputs="edges.biological_process_protein",
    name="biological_process_protein",
    tags=["silver"],
)
