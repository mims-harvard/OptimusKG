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
                pl.lit("biological_process_protein").alias("relation"),
                pl.lit(True).alias("undirected"),
                pl.col("source").drop_nulls().unique().sort().alias("sources"),
                pl.col("evidence").drop_nulls().unique().sort(),
                pl.col("aspect").drop_nulls().unique().sort().alias("aspect"),
                pl.col("geneProduct").drop_nulls().unique().sort(),
                pl.col("ecoId").drop_nulls().unique().sort().alias("ecoIds"),
            ]
        )
        .filter(pl.col("aspect") == ["P"])
        .select(  # P = biological process
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


biological_process_protein_node = node(
    run,
    inputs={
        "target": "bronze.opentargets.target",
    },
    outputs="edges.biological_process_protein",
    name="biological_process_protein",
    tags=["silver"],
)
