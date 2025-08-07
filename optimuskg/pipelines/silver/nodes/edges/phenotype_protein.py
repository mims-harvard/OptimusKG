import polars as pl
from kedro.pipeline import node


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
            pl.struct(
                [
                    pl.lit(["opentargets"]).alias("sources"),
                    pl.col("metadata").struct.field("score").alias("evidenceScore"),
                    pl.col("metadata")
                    .struct.field("evidenceCount")
                    .alias("evidenceCount"),
                    pl.lit("associated with").alias(
                        "relationType"
                    ),  # TODO: change this literal to "associated with" using the evidenceScore/evidenceCount columns.
                ]
            ).alias("opentargets_properties"),
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
        .unique(subset=["from", "to"])
    )

    merged = (
        opentargets_phenotype_protein.unique(subset=["from", "to"])
        .join(disgenet_phenotype_protein, on=["from", "to"], how="left")
        .select(
            [
                pl.col("from"),
                pl.col("to"),
                pl.lit(False).alias("undirected"),
                pl.lit("phenotype_protein").alias("relation"),
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
                            .alias("sources"),
                            pl.col("disgenet_properties").struct.field("relationType"),
                        ]
                    )
                )
                .otherwise(pl.col("opentargets_properties"))
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
