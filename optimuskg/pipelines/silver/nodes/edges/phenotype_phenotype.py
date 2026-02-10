import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node, Relation


def run(
    hp_relations: pl.DataFrame,
) -> pl.DataFrame:
    return (
        hp_relations.select(
            pl.col("parent").alias("from"),
            pl.col("child").alias("to"),
            pl.lit(Edge.format_label(Node.PHENOTYPE, Node.PHENOTYPE)).alias("label"),
            pl.lit(Relation.PARENT).alias("relation"),
            pl.lit(False).alias("undirected"),
            pl.struct(  # TODO: we can add more metadata merging with opentargets disease_phenotype and disease dataset
                [
                    pl.lit(["HP"]).alias("direct_sources"),
                    pl.lit([]).cast(pl.List(pl.String)).alias("indirect_sources"),
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
    outputs="edges.phenotype_phenotype",
    name="phenotype_phenotype",
    tags=["silver"],
)
