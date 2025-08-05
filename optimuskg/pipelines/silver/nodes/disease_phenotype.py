import polars as pl
from kedro.pipeline import node


def run(
    disease_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    return (
        disease_phenotype.filter(~pl.col("disease").str.contains("HP"))
        .with_columns(
            pl.col("disease").alias("from"),
            pl.col("phenotype").alias("to"),
            pl.col("metadata").struct.field("evidence").alias("properties"),
        )
        .explode("properties")
        .unnest("properties")
        .sort(by="onset")
        .group_by(["from", "to"])
        .agg(
            [
                pl.col("aspect").drop_nulls().unique().alias("aspect"),
                pl.col("bioCuration").drop_nulls().unique().alias("bioCuration"),
                pl.col("evidenceType").drop_nulls().unique().alias("evidenceType"),
                pl.col("frequency").drop_nulls().unique().alias("frequency"),
                pl.concat_list("modifiers")
                .flatten()
                .drop_nulls()
                .unique()
                .alias("modifiers"),
                pl.concat_list("onset").flatten().drop_nulls().unique().alias("onset"),
                pl.col("qualifierNot").any().alias("qualifierNot"),
                pl.when(~pl.col("qualifierNot"))
                .then(pl.lit("phenotype present"))
                .otherwise(pl.lit("phenotype absent"))
                .unique()
                .alias("relationType"),
                pl.concat_list("references")
                .flatten()
                .drop_nulls()
                .unique()
                .alias("references"),
                pl.col("sex").drop_nulls().unique().alias("sexes"),
                pl.col("resource").drop_nulls().unique().alias("sources"),
            ]
        )
        .select(
            pl.col("from"),
            pl.col("to"),
            pl.lit("disease_phenotype").alias("relation"),
            pl.lit(True).alias("undirected"),
            pl.struct(
                [
                    pl.col("aspect"),
                    pl.col("bioCuration"),
                    pl.col("evidenceType"),
                    pl.col("frequency"),
                    pl.col("modifiers"),
                    pl.col("onset"),
                    pl.col("qualifierNot"),
                    pl.col("relationType"),
                    pl.col("references"),
                    pl.col("sexes"),
                    pl.col("sources"),
                ]
            ).alias("properties"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


disease_phenotype_node = node(
    run,
    inputs={
        "disease_phenotype": "bronze.opentargets.disease_phenotype",
    },
    outputs="disease_phenotype",
    name="disease_phenotype",
    tags=["silver"],
)
