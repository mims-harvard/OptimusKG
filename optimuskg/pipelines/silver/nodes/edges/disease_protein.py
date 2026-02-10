import logging

import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import (
    Edge,
    Node,
    Relation,
    Source,
    resolve_sources,
)

logger = logging.getLogger(__name__)


def run(  # noqa: PLR0913
    # opentargets
    target_disease_associations: pl.DataFrame,
    disease: pl.DataFrame,
    target: pl.DataFrame,
    # disgenet
    disgenet_diseases: pl.DataFrame,
) -> pl.DataFrame:
    diesase_id_umls_map = (
        disease.select(
            [
                pl.col("id"),
                pl.col("metadata").struct.field("db_xrefs").alias("db_xrefs"),
            ]
        )
        .explode("db_xrefs")
        .filter(pl.col("db_xrefs").is_not_null())
        .filter(pl.col("db_xrefs").str.starts_with("UMLS"))
        .group_by("id")
        .agg([pl.col("db_xrefs").first().str.replace("UMLS:", "").alias("umls_id")])
        .sort("umls_id")
    )
    opentargets_disease_protein = (
        target_disease_associations.filter(~pl.col("disease_id").str.contains("HP"))
        .with_columns(
            pl.col("disease_id").alias("from"),
            pl.col("target_id").alias("to"),
            pl.lit(Edge.format_label(Node.DISEASE, Node.PROTEIN)).alias("label"),
            pl.lit(Relation.ASSOCIATED_WITH).alias(
                "relation"
            ),  # TODO: change this literal to "associated with" using the evidence_score/evidence_count columns.
            pl.lit(True).alias("undirected"),
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
        .select(["from", "to", "label", "relation", "undirected", "opentargets_props"])
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )

    disgenet_disease_protein = (
        disgenet_diseases.select(
            "gene_symbol",
            "disease_id",
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
        .join(
            diesase_id_umls_map, left_on="disease_id", right_on="umls_id", how="inner"
        )
        .join(
            target.select([pl.col("id").alias("target_id"), pl.col("approved_symbol")]),
            left_on="gene_symbol",
            right_on="approved_symbol",
            how="inner",
        )
        .select(
            pl.col("id").alias("from"),
            pl.col("target_id").alias("to"),
            pl.col("relation"),
            pl.col("disgenet_props"),
        )
        .sort(by=["from", "to"])
        .unique(subset=["from", "to"])
    )

    # Here we merge the metadata from opentargets and disgenet for the same (disease, protein) pairs.
    merged = (
        opentargets_disease_protein.join(
            disgenet_disease_protein, on=["from", "to"], how="left"
        )
        .with_columns(
            [
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
                .alias("properties")
            ]
        )
        .drop("disgenet_props", "opentargets_props", "relation_right")
        .unique(subset=["from", "to"])
        .sort(["from", "to"])
    )

    # TODO: We need to merge the same (gene, disease) pairs that have (obviously) the same name/symbol, but different IDs.
    # We keep only one of them, and aggregate the metadata columns from opentargets and disgenet.
    #
    # Example:
    # - (x_name:SCO2, y_name:cardioencephalomyopathy) but (x_id:ENSG00000123456, y_id:MONDO_0011451) and (x_id:NCBIGene:9997, y_id:MONDO_0011451)
    # See: ENSG00000123456 and NCBIGene:9997 are different IDs for the same gene.

    return merged


disease_protein_node = node(
    run,
    inputs={
        # opentargets
        "target_disease_associations": "bronze.opentargets.target_disease_associations",
        "disease": "bronze.opentargets.disease",
        "target": "bronze.opentargets.target",
        # disgenet
        "disgenet_diseases": "bronze.disgenet.diseases",
    },
    outputs="edges.disease_protein",
    name="disease_protein",
    tags=["silver"],
)
