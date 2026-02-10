import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import (
    Edge,
    Node,
    Relation,
    Source,
    resolve_sources,
)


def run(
    target_disease_associations: pl.DataFrame,
    target: pl.DataFrame,
    hp_xrefs: pl.DataFrame,
    disgenet_phenotypes: pl.DataFrame,
) -> pl.DataFrame:
    opentargets_phenotype_protein = (
        target_disease_associations.filter(pl.col("disease_id").str.contains("HP"))
        .select(
            pl.col("disease_id").alias("from"),
            pl.col("target_id").alias("to"),
            pl.lit(Relation.ASSOCIATED_WITH).alias(
                "relation"
            ),  # TODO: change this literal to "associated with" using the evidence_score/evidence_count columns.
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit([Source.OPENTARGETS])
                            .cast(pl.List(pl.String))
                            .alias("direct"),
                            pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                        ]
                    ).alias("sources"),
                    pl.col("metadata").struct.field("score").alias("evidence_score"),
                    pl.col("metadata")
                    .struct.field("evidence_count")
                    .alias("evidence_count"),
                ]
            ).alias("opentargets_props"),
        )
        .unique(subset=["from", "to"])
    )

    disgenet_phenotype_protein = (
        target.select("id", "approved_symbol")
        .join(
            disgenet_phenotypes,
            left_on="approved_symbol",
            right_on="gene_symbol",
            how="inner",
        )
        .join(hp_xrefs, left_on="disease_id", right_on="ontology_id", how="inner")
        .select(
            pl.col("hp_id").alias("from"),
            pl.col("id").alias("to"),
            pl.lit(Relation.ASSOCIATED_WITH).alias(
                "relation"
            ),  # TODO: change this literal to "associated with" using the disgenet_score/evidence_index column.
            pl.struct(
                [
                    pl.col("dsi").cast(pl.Float64).alias("disease_specificity_index"),
                    pl.col("dpi").cast(pl.Float64).alias("disease_pleiotropy_index"),
                    pl.col("ei").cast(pl.Float64).alias("evidence_index"),
                    pl.col("score").alias("disgenet_score"),
                    pl.col("year_initial").alias("year_initial"),
                    pl.col("year_final").alias("year_final"),
                    pl.col("nof_pmids").cast(pl.Int16).alias("number_of_pmids"),
                    pl.col("nof_snps").cast(pl.Int16).alias("number_of_snps"),
                    pl.struct(
                        [
                            pl.lit([Source.DISGENET])
                            .cast(pl.List(pl.String))
                            .alias("direct"),
                            pl.col("source")
                            .str.split(";")
                            .cast(pl.List(pl.String))
                            .map_elements(
                                resolve_sources, return_dtype=pl.List(pl.String)
                            )
                            .alias("indirect"),
                        ]
                    ).alias("sources"),
                ]
            ).alias("disgenet_props"),
        )
        .unique(subset=["from", "to"])
    )

    merged = (
        opentargets_phenotype_protein.unique(subset=["from", "to"])
        .join(disgenet_phenotype_protein, on=["from", "to"], how="left")
        .select(
            [
                pl.col("from"),
                pl.col("to"),
                pl.lit(Edge.format_label(Node.PHENOTYPE, Node.PROTEIN)).alias("label"),
                pl.col("relation"),
                pl.lit(False).alias("undirected"),
                pl.when(pl.col("disgenet_props").is_not_null())
                .then(
                    pl.struct(
                        [
                            *[
                                pl.col("opentargets_props").struct.field(f)
                                for f in ["evidence_score", "evidence_count"]
                            ],
                            *[
                                pl.col("disgenet_props").struct.field(f).alias(f)
                                for f in [
                                    "disease_specificity_index",
                                    "disease_pleiotropy_index",
                                    "evidence_index",
                                    "disgenet_score",
                                    "year_initial",
                                    "year_final",
                                    "number_of_pmids",
                                    "number_of_snps",
                                ]
                            ],
                            pl.struct(
                                [
                                    pl.concat_list(
                                        [
                                            pl.col("opentargets_props")
                                            .struct.field("sources")
                                            .struct.field("direct"),
                                            pl.col("disgenet_props")
                                            .struct.field("sources")
                                            .struct.field("direct"),
                                        ]
                                    )
                                    .list.unique()
                                    .alias("direct"),
                                    pl.concat_list(
                                        [
                                            pl.col("opentargets_props")
                                            .struct.field("sources")
                                            .struct.field("indirect"),
                                            pl.col("disgenet_props")
                                            .struct.field("sources")
                                            .struct.field("indirect"),
                                        ]
                                    )
                                    .list.unique()
                                    .alias("indirect"),
                                ]
                            ).alias("sources"),
                        ]
                    )
                )
                .otherwise(pl.col("opentargets_props"))
                .alias("properties"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(["from", "to"])
    )

    return merged


phenotype_protein_node = node(
    run,
    inputs={
        "target_disease_associations": "bronze.opentargets.target_disease_associations",
        "target": "bronze.opentargets.target",
        "hp_xrefs": "bronze.ontology.hp_xrefs",
        "disgenet_phenotypes": "bronze.disgenet.disgenet_phenotypes",
    },
    outputs="edges.phenotype_protein",
    name="phenotype_protein",
    tags=["silver"],
)
