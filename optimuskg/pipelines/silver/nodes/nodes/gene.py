import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Node


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
            pl.col("transcript_ids"),
            pl.col("canonical_transcript"),
            pl.col("canonical_exons"),
            pl.col("genomic_location"),
            pl.col("alternative_genes"),
            pl.col("approved_name").alias("name"),
            pl.col("hallmarks")
            .struct.field("attributes")
            .alias("hallmarks_attributes"),
            pl.col("hallmarks")
            .struct.field("cancer_hallmarks")
            .alias("cancer_hallmarks"),
            pl.col("synonyms"),
            pl.col("symbol_synonyms"),
            pl.col("name_synonyms"),
            pl.col("function_descriptions"),
            pl.col("subcellular_locations"),
            pl.col("target_class"),
            pl.col("obsolete_symbols"),
            pl.col("obsolete_names"),
            pl.col("constraint").alias("constraint_scores"),
            pl.col("tep").alias("target_enabling_package"),
            pl.col("protein_ids").alias("associated_proteins"),
            pl.col("db_xrefs").alias("xrefs"),
            pl.col("chemical_probes"),
            pl.col("homologues"),
            pl.col("tractability"),
            pl.col("safety_liabilities"),
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
                                .then(pl.lit(["disgenet"]))
                                .otherwise(pl.lit(["opentargets", "BGEE"]))
                                .alias("direct"),
                                pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                            ]
                        ).alias("sources"),
                        pl.coalesce([pl.col("symbol"), pl.col("gene_symbol")]).alias(
                            "symbol"
                        ),
                        pl.col("biotype"),
                        pl.col("transcript_ids"),
                        pl.col("canonical_transcript"),
                        pl.col("canonical_exons"),
                        pl.col("genomic_location"),
                        pl.col("alternative_genes"),
                        pl.col("name"),
                        pl.col("hallmarks_attributes"),
                        pl.col("cancer_hallmarks"),
                        pl.col("synonyms"),
                        pl.col("symbol_synonyms"),
                        pl.col("name_synonyms"),
                        pl.col("function_descriptions"),
                        pl.col("subcellular_locations"),
                        pl.col("target_class"),
                        pl.col("obsolete_symbols"),
                        pl.col("obsolete_names"),
                        pl.col("constraint_scores"),
                        pl.col("target_enabling_package"),
                        pl.col("associated_proteins"),
                        pl.col("xrefs"),
                        pl.col("chemical_probes"),
                        pl.col("homologues"),
                        pl.col("tractability"),
                        pl.col("safety_liabilities"),
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
