import polars as pl
from kedro.pipeline import node


def run(  # noqa: PLR0913
    anatomy_protein: pl.DataFrame,
    biological_process_protein: pl.DataFrame,
    exposure_protein: pl.DataFrame,
    drug_protein: pl.DataFrame,
    cellular_component_protein: pl.DataFrame,
    molecular_function_protein: pl.DataFrame,
    pathway_protein: pl.DataFrame,
    disease_protein: pl.DataFrame,
    phenotype_protein: pl.DataFrame,
    gene_expressions_in_anatomy: pl.DataFrame,
    target: pl.DataFrame,
) -> pl.DataFrame:
    flattened_target = target.unnest(
        "metadata"
    ).select(  # TODO: this flattened target should be the output of the bronze layer
        [
            pl.col("id"),
            pl.col("approved_symbol").alias("symbol"),
            pl.col("biotype"),
            pl.col("transcriptIds"),
            pl.col("canonicalTranscript")
            .struct.json_encode()
            .alias("canonicalTranscriptEncoded"),
            pl.col("canonicalExons"),
            pl.col("genomicLocation")
            .struct.json_encode()
            .alias("genomicLocationEncoded"),
            pl.col("alternativeGenes"),
            pl.col("approved_name").alias("name"),
            pl.col("hallmarks")
            .struct.field("attributes")
            .list.eval(pl.element().struct.json_encode())
            .alias("hallmarksAttributesEncoded"),
            pl.col("hallmarks")
            .struct.field("cancerHallmarks")
            .list.eval(pl.element().struct.json_encode())
            .alias("cancerHallmarksEncoded"),
            pl.col("synonyms")
            .list.eval(pl.element().struct.json_encode())
            .alias("synonymsEncoded"),
            pl.col("symbolSynonyms")
            .list.eval(pl.element().struct.json_encode())
            .alias("symbolSynonymsEncoded"),
            pl.col("nameSynonyms")
            .list.eval(pl.element().struct.json_encode())
            .alias("nameSynonymsEncoded"),
            pl.col("functionDescriptions"),
            pl.col("subcellularLocations")
            .list.eval(pl.element().struct.json_encode())
            .alias("subcellularLocationsEncoded"),
            pl.col("targetClass")
            .list.eval(pl.element().struct.json_encode())
            .alias("targetClassEncoded"),
            pl.col("obsoleteSymbols")
            .list.eval(pl.element().struct.json_encode())
            .alias("obsoleteSymbolsEncoded"),
            pl.col("obsoleteNames")
            .list.eval(pl.element().struct.json_encode())
            .alias("obsoleteNamesEncoded"),
            pl.col("constraint")
            .list.eval(pl.element().struct.json_encode())
            .alias("constraintScoresEncoded"),
            pl.col("tep").struct.json_encode().alias("targetEnablingPackageEncoded"),
            pl.col("proteinIds")
            .list.eval(pl.element().struct.json_encode())
            .alias("associatedProteinsEncoded"),
            pl.col("dbXrefs")
            .list.eval(pl.element().struct.json_encode())
            .alias("xrefsEncoded"),
            pl.col("chemicalProbes")
            .list.eval(pl.element().struct.json_encode())
            .alias("chemicalProbesEncoded"),
            pl.col("homologues")
            .list.eval(pl.element().struct.json_encode())
            .alias("homologuesEncoded"),
            pl.col("tractability")
            .list.eval(pl.element().struct.json_encode())
            .alias("tractabilityEncoded"),
            pl.col("safetyLiabilities")
            .list.eval(pl.element().struct.json_encode())
            .alias("safetyLiabilitiesEncoded"),
            pl.col("tss").alias("transcriptionStartSite"),
        ]
    )
    return (
        pl.concat(
            [
                anatomy_protein.select(pl.col("to").alias("id")),
                biological_process_protein.select(pl.col("to").alias("id")),
                cellular_component_protein.select(pl.col("to").alias("id")),
                disease_protein.select(pl.col("to").alias("id")),
                drug_protein.select(pl.col("to").alias("id")),
                exposure_protein.select(pl.col("to").alias("id")),
                molecular_function_protein.select(pl.col("to").alias("id")),
                pathway_protein.select(pl.col("to").alias("id")),
                phenotype_protein.select(pl.col("to").alias("id")),
            ]
        )
        .unique(subset="id")
        .join(flattened_target, left_on="id", right_on="id", how="left")
        .join(
            gene_expressions_in_anatomy.select(
                ["gene_id", pl.col("gene_name").alias("gene_symbol")]
            ).unique(subset="gene_id"),
            left_on="id",
            right_on="gene_id",
            how="inner",
        )
        .select(
            [
                pl.col("id"),
                pl.lit("gene").alias("node_type"),
                pl.struct(
                    [
                        pl.lit(["opentarges", "BGEE"]).alias("sources"),
                        pl.coalesce([pl.col("symbol"), pl.col("gene_symbol")]).alias(
                            "symbol"
                        ),
                        pl.col("biotype"),
                        pl.col("transcriptIds"),
                        pl.col("canonicalTranscriptEncoded"),
                        pl.col("canonicalExons"),
                        pl.col("genomicLocationEncoded"),
                        pl.col("alternativeGenes"),
                        pl.col("name"),
                        pl.col("hallmarksAttributesEncoded"),
                        pl.col("cancerHallmarksEncoded"),
                        pl.col("synonymsEncoded"),
                        pl.col("symbolSynonymsEncoded"),
                        pl.col("nameSynonymsEncoded"),
                        pl.col("functionDescriptions"),
                        pl.col("subcellularLocationsEncoded"),
                        pl.col("targetClassEncoded"),
                        pl.col("obsoleteSymbolsEncoded"),
                        pl.col("obsoleteNamesEncoded"),
                        pl.col("constraintScoresEncoded"),
                        pl.col("targetEnablingPackageEncoded"),
                        pl.col("associatedProteinsEncoded"),
                        pl.col("xrefsEncoded"),
                        pl.col("chemicalProbesEncoded"),
                        pl.col("homologuesEncoded"),
                        pl.col("tractabilityEncoded"),
                        pl.col("safetyLiabilitiesEncoded"),
                        pl.col("transcriptionStartSite"),
                    ]
                ).alias("properties"),
            ]
        )
        .sort(by="id")
    )


gene_node = node(
    run,
    inputs={
        "anatomy_protein": "silver.edges.anatomy_protein",
        "biological_process_protein": "silver.edges.biological_process_protein",
        "exposure_protein": "silver.edges.exposure_protein",
        "drug_protein": "silver.edges.drug_protein",
        "cellular_component_protein": "silver.edges.cellular_component_protein",
        "molecular_function_protein": "silver.edges.molecular_function_protein",
        "pathway_protein": "silver.edges.pathway_protein",
        "disease_protein": "silver.edges.disease_protein",
        "phenotype_protein": "silver.edges.phenotype_protein",
        "gene_expressions_in_anatomy": "bronze.bgee.gene_expressions_in_anatomy",
        "target": "bronze.opentargets.target",
    },
    outputs="nodes.gene",
    name="gene",
    tags=["silver"],
)
