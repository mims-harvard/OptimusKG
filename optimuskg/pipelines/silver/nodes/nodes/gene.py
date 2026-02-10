import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Node, Source


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
    protein_protein: pl.DataFrame,
) -> pl.DataFrame:
    flattened_target = target.unnest(
        "metadata"
    ).select(  # TODO: this flattened target should be the output of the bronze layer
        [
            pl.col("id"),
            pl.col("approved_symbol").alias("symbol"),
            pl.col("biotype"),
            pl.col("transcript_ids").alias("transcript_ids"),
            pl.col("canonical_transcript")
            .struct.json_encode()
            .alias("canonical_transcript_encoded"),
            pl.col("canonical_exons").alias("canonical_exons"),
            pl.col("genomic_location")
            .struct.json_encode()
            .alias("genomic_location_encoded"),
            pl.col("alternative_genes").alias("alternative_genes"),
            pl.col("approved_name").alias("name"),
            pl.col("hallmarks")
            .struct.field("attributes")
            .list.eval(pl.element().struct.json_encode())
            .alias("hallmarks_attributes_encoded"),
            pl.col("hallmarks")
            .struct.field("cancer_hallmarks")
            .list.eval(pl.element().struct.json_encode())
            .alias("cancer_hallmarks_encoded"),
            pl.col("synonyms")
            .list.eval(pl.element().struct.json_encode())
            .alias("synonyms_encoded"),
            pl.col("symbol_synonyms")
            .list.eval(pl.element().struct.json_encode())
            .alias("symbol_synonyms_encoded"),
            pl.col("name_synonyms")
            .list.eval(pl.element().struct.json_encode())
            .alias("name_synonyms_encoded"),
            pl.col("function_descriptions").alias("function_descriptions"),
            pl.col("subcellular_locations")
            .list.eval(pl.element().struct.json_encode())
            .alias("subcellular_locations_encoded"),
            pl.col("target_class")
            .list.eval(pl.element().struct.json_encode())
            .alias("target_class_encoded"),
            pl.col("obsolete_symbols")
            .list.eval(pl.element().struct.json_encode())
            .alias("obsolete_symbols_encoded"),
            pl.col("obsolete_names")
            .list.eval(pl.element().struct.json_encode())
            .alias("obsolete_names_encoded"),
            pl.col("constraint")
            .list.eval(pl.element().struct.json_encode())
            .alias("constraint_scores_encoded"),
            pl.col("tep").struct.json_encode().alias("target_enabling_package_encoded"),
            pl.col("protein_ids")
            .list.eval(pl.element().struct.json_encode())
            .alias("associated_proteins_encoded"),
            pl.col("db_xrefs")
            .list.eval(pl.element().struct.json_encode())
            .alias("xrefs_encoded"),
            pl.col("chemical_probes")
            .list.eval(pl.element().struct.json_encode())
            .alias("chemical_probes_encoded"),
            pl.col("homologues")
            .list.eval(pl.element().struct.json_encode())
            .alias("homologues_encoded"),
            pl.col("tractability")
            .list.eval(pl.element().struct.json_encode())
            .alias("tractability_encoded"),
            pl.col("safety_liabilities")
            .list.eval(pl.element().struct.json_encode())
            .alias("safety_liabilities_encoded"),
            pl.col("tss").alias("transcription_start_site"),
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
                protein_protein.select(pl.col("to").alias("id")),
                protein_protein.select(pl.col("from").alias("id")),
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
            how="left",
        )
        .select(
            [
                pl.col("id"),
                pl.lit(Node.GENE).alias("label"),
                pl.struct(
                    [
                        pl.struct(
                            [
                                pl.when(pl.col("id").str.starts_with("NCBIGene"))
                                .then(
                                    pl.lit([Source.DISGENET]).cast(pl.List(pl.String))
                                )
                                .otherwise(
                                    pl.lit([Source.OPENTARGETS, Source.BGEE]).cast(
                                        pl.List(pl.String)
                                    )
                                )
                                .alias("direct"),
                                pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                            ]
                        ).alias("sources"),
                        pl.coalesce([pl.col("symbol"), pl.col("gene_symbol")]).alias(
                            "symbol"
                        ),
                        pl.col("biotype"),
                        pl.col("transcript_ids"),
                        pl.col("canonical_transcript_encoded"),
                        pl.col("canonical_exons"),
                        pl.col("genomic_location_encoded"),
                        pl.col("alternative_genes"),
                        pl.col("name"),
                        pl.col("hallmarks_attributes_encoded"),
                        pl.col("cancer_hallmarks_encoded"),
                        pl.col("synonyms_encoded"),
                        pl.col("symbol_synonyms_encoded"),
                        pl.col("name_synonyms_encoded"),
                        pl.col("function_descriptions"),
                        pl.col("subcellular_locations_encoded"),
                        pl.col("target_class_encoded"),
                        pl.col("obsolete_symbols_encoded"),
                        pl.col("obsolete_names_encoded"),
                        pl.col("constraint_scores_encoded"),
                        pl.col("target_enabling_package_encoded"),
                        pl.col("associated_proteins_encoded"),
                        pl.col("xrefs_encoded"),
                        pl.col("chemical_probes_encoded"),
                        pl.col("homologues_encoded"),
                        pl.col("tractability_encoded"),
                        pl.col("safety_liabilities_encoded"),
                        pl.col("transcription_start_site"),
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
        "protein_protein": "silver.edges.protein_protein",
    },
    outputs="nodes.gene",
    name="gene",
    tags=["silver"],
)
