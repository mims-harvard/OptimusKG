import logging

import polars as pl
from kedro.pipeline import node

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
            [pl.col("id"), pl.col("metadata").struct.field("dbXRefs").alias("dbXRefs")]
        )
        .explode("dbXRefs")
        .filter(pl.col("dbXRefs").is_not_null())
        .filter(pl.col("dbXRefs").str.starts_with("UMLS"))
        .group_by("id")
        .agg([pl.col("dbXRefs").first().str.replace("UMLS:", "").alias("umls_id")])
        .sort("umls_id")
    )
    opentargets_disease_protein = (
        target_disease_associations.filter(~pl.col("disease_id").str.contains("HP"))
        .with_columns(
            pl.col("disease_id").alias("from"),
            pl.col("target_id").alias("to"),
            pl.lit("disease_protein").alias("relation"),
            pl.lit(True).alias("undirected"),
            pl.struct(
                [
                    pl.lit(["opentargets"]).alias("sources"),
                    pl.col("metadata").struct.field("score").alias("evidenceScore"),
                    pl.col("metadata").struct.field("evidenceCount"),
                    pl.lit("associated with").alias(
                        "relationType"
                    ),  # TODO: change this literal to "associated with" using the evidenceScore/evidenceCount columns.
                ]
            ).alias("opentargets_properties"),
        )
        .select(["from", "to", "relation", "undirected", "opentargets_properties"])
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )

    disgenet_disease_protein = (
        disgenet_diseases.select(
            "gene_symbol",
            "disease_id",
            pl.struct(
                [
                    pl.col("dsi").cast(pl.Float64).alias("diseaseSpecificityIndex"),
                    pl.col("dpi").cast(pl.Float64).alias("diseasePleiotropyIndex"),
                    pl.col("ei").cast(pl.Float64).alias("evidenceIndex"),
                    pl.col("score").alias("disgenetScore"),
                    pl.col("year_initial").alias("yearInitial"),
                    pl.col("year_final").alias("yearFinal"),
                    pl.col("nof_pmids").cast(pl.Int16).alias("numberOfPmids"),
                    pl.col("nof_snps").cast(pl.Int16).alias("numberOfSnps"),
                    pl.col("source")
                    .str.split(";")
                    .cast(pl.List(pl.Utf8))
                    .alias("sources"),
                    pl.lit("associated with").alias(
                        "relationType"
                    ),  # TODO: change this literal to "associated with" using the disgenetScore/evidenceIndex column.
                ]
            ).alias("disgenet_properties"),
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
            pl.col("disgenet_properties"),
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
                pl.when(pl.col("disgenet_properties").is_not_null())
                .then(
                    pl.struct(
                        [
                            *[
                                pl.col("opentargets_properties").struct.field(f)
                                for f in ["evidenceScore", "evidenceCount"]
                            ],
                            *[
                                pl.col("disgenet_properties").struct.field(f).alias(f)
                                for f in [
                                    "diseaseSpecificityIndex",
                                    "diseasePleiotropyIndex",
                                    "evidenceIndex",
                                    "disgenetScore",
                                    "yearInitial",
                                    "yearFinal",
                                    "numberOfPmids",
                                    "numberOfSnps",
                                ]
                            ],
                            pl.concat_list(
                                [
                                    pl.col("opentargets_properties").struct.field(
                                        "sources"
                                    ),
                                    pl.col("disgenet_properties").struct.field(
                                        "sources"
                                    ),
                                ]
                            )
                            .list.unique()
                            .list.sort()
                            .alias("sources"),
                            pl.col("disgenet_properties").struct.field("relationType"),
                        ]
                    )
                )
                .otherwise(pl.col("opentargets_properties"))
                .alias("properties")
            ]
        )
        .drop("disgenet_properties", "opentargets_properties")
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
