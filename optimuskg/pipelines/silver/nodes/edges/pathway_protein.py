import polars as pl
from kedro.pipeline import node


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
                pl.lit("pathway_protein").alias("relation"),
                pl.lit(True).alias("undirected"),
                pl.struct(
                    [
                        pl.lit(["opentargets"]).alias("sources"),
                        pl.lit("interacts with").alias("relation_type"),
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
