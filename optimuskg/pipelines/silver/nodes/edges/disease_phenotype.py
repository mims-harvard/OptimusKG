import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import (
    Edge,
    Node,
    Relation,
    resolve_relation,
)


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
                pl.col("bio_curation").drop_nulls().unique().alias("bio_curation"),
                pl.col("evidence_type").drop_nulls().unique().alias("evidence_type"),
                pl.col("frequency").drop_nulls().unique().alias("frequency"),
                pl.concat_list("modifiers")
                .flatten()
                .drop_nulls()
                .unique()
                .alias("modifiers"),
                pl.concat_list("onset").flatten().drop_nulls().unique().alias("onset"),
                pl.col("qualifier_not").any().alias("qualifier_not"),
                pl.when(~pl.col("qualifier_not"))
                .then(pl.lit(Relation.PHENOTYPE_PRESENT))
                .otherwise(pl.lit(Relation.PHENOTYPE_ABSENT))
                .unique()
                .alias("relation"),
                pl.concat_list("references")
                .flatten()
                .drop_nulls()
                .unique()
                .alias("references"),
                pl.col("sex").drop_nulls().unique().alias("sexes"),
                pl.col("resource").drop_nulls().unique().alias("indirect_sources"),
            ]
        )
        .select(
            pl.col("from"),
            pl.col("to"),
            pl.lit(Edge.format_label(Node.DISEASE, Node.PHENOTYPE)).alias("label"),
            pl.col("relation").map_elements(resolve_relation, return_dtype=pl.String),
            pl.lit(True).alias("undirected"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit(["opentargets"]).alias("direct"),
                            pl.col("indirect_sources").alias("indirect"),
                        ]
                    ).alias("sources"),
                    pl.col("aspect"),
                    pl.col("bio_curation"),
                    pl.col("evidence_type"),
                    pl.col("frequency"),
                    pl.col("modifiers"),
                    pl.col("onset"),
                    pl.col("qualifier_not"),
                    pl.col("references"),
                    pl.col("sexes"),
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
    outputs="edges.disease_phenotype",
    name="disease_phenotype",
    tags=["silver"],
)
