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
                pl.lit("molecular_function_protein").alias("relation"),
                pl.lit(True).alias("undirected"),
                pl.col("source").unique().sort().alias("sources"),
                pl.col("evidence").unique().sort(),
                pl.col("aspect").unique().sort().alias("aspect"),
                pl.col("geneProduct").unique().sort(),
                pl.col("ecoId").drop_nulls().unique().sort().alias("ecoIds"),
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


molecular_function_protein_node = node(
    run,
    inputs={
        "target": "bronze.opentargets.target",
    },
    outputs="edges.molecular_function_protein",
    name="molecular_function_protein",
    tags=["silver"],
)
