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
                pl.lit("disease_phenotype").alias("relation"),
                pl.lit(True).alias("undirected"),
                pl.col("aspect").unique().alias("aspect"),
                pl.col("bioCuration").unique().alias("bioCuration"),
                pl.col("evidenceType").unique().alias("evidenceType"),
                pl.col("frequency").unique().alias("frequency"),
                pl.concat_list("modifiers").flatten().unique().alias("modifiers"),
                pl.concat_list("onset").flatten().unique().alias("onset"),
                pl.col("qualifierNot").any().alias("qualifierNot"),
                pl.when(~pl.col("qualifierNot"))
                .then(pl.lit("phenotype present"))
                .otherwise(pl.lit("phenotype absent"))
                .unique()
                .alias("relationType"),
                pl.concat_list("references").flatten().unique().alias("references"),
                pl.col("sex").unique().alias("sex"),
                pl.col("resource").unique().alias("source"),
            ]
        )
        .with_columns(
            [
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
                        pl.col("sex"),
                        pl.col("source"),
                    ]
                ).alias("properties")
            ]
        )
        .select(["from", "to", "relation", "undirected", "properties"])
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
