import polars as pl
from kedro.pipeline import node


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
                pl.lit("cellular_component_protein").alias("relation"),
                pl.lit(True).alias("undirected"),
                pl.col("source").unique().alias("sources"),
                pl.col("evidence").unique(),
                pl.col("aspect").unique().alias("aspect"),
                pl.col("geneProduct").unique(),
                pl.col("ecoId").unique().alias("ecoIds"),
            ]
        )
        .filter(pl.col("aspect") == ["C"])  # C = cellular component
        .select(
            [
                pl.col("id").alias("from"),
                pl.col("target_id").alias("to"),
                pl.col("relation"),
                pl.col("undirected"),
                pl.struct(
                    [
                        pl.col("sources"),
                        pl.col("evidence"),
                        pl.col("geneProduct"),
                        pl.col("ecoIds"),
                        pl.lit("interacts with").alias("relationType"),
                    ]
                ).alias("properties"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


cellular_component_protein_node = node(
    run,
    inputs={
        "target": "bronze.opentargets.target",
    },
    outputs="cellular_component_protein",
    name="cellular_component_protein",
    tags=["silver"],
)
