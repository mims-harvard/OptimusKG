import polars as pl
from kedro.pipeline import node


def run(
    hp_relations: pl.DataFrame,
) -> pl.DataFrame:
    return (
        hp_relations.select(
            pl.col("parent").alias("from"),
            pl.col("child").alias("to"),
            pl.lit("phenotype_phenotype").alias("relation"),
            pl.lit(False).alias("undirected"),
            pl.struct(  # TODO: we can add more metadata merging with opentargets disease_phenotype and disease dataset
                [
                    pl.lit(["HP"]).alias("sources"),
                    pl.lit("parent").alias("relationType"),
                ]
            ).alias("properties"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


phenotype_phenotype_node = node(
    run,
    inputs={
        "hp_relations": "bronze.ontology.hp_relations",
    },
    outputs="phenotype_phenotype",
    name="phenotype_phenotype",
    tags=["silver"],
)
