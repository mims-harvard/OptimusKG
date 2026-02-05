import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node, Relation


def run(
    target: pl.DataFrame,
) -> pl.DataFrame:
    return (
        target.select([pl.col("id"), pl.col("metadata").struct.field("pathways")])
        .explode("pathways")
        .unnest("pathways")
        .filter(pl.col("pathway_id").is_not_null())
        .select(
            [
                ("REACT:" + pl.col("pathway_id")).alias(
                    "from"
                ),  # NOTE: we need to add the REACT prefix for biolink mapping
                pl.col("id").alias("to"),
                pl.lit(Edge.format_label(Node.PATHWAY, Node.PROTEIN)).alias("label"),
                pl.lit(Relation.INTERACTS_WITH).alias("relation"),
                pl.lit(True).alias("undirected"),
                pl.struct(
                    [
                        pl.lit(["opentargets"]).alias("sources"),
                    ]
                ).alias("properties"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


pathway_protein_node = node(
    run,
    inputs={
        "target": "bronze.opentargets.target",
    },
    outputs="edges.pathway_protein",
    name="pathway_protein",
    tags=["silver"],
)
