import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Node, Source


def run(  # noqa: PLR0913
    anatomy_gene: pl.DataFrame,
    biological_process_gene: pl.DataFrame,
    exposure_gene: pl.DataFrame,
    drug_gene: pl.DataFrame,
    cellular_component_gene: pl.DataFrame,
    molecular_function_gene: pl.DataFrame,
    pathway_gene: pl.DataFrame,
    disease_gene: pl.DataFrame,
    phenotype_gene: pl.DataFrame,
    gene_expressions_in_anatomy: pl.DataFrame,
    target: pl.DataFrame,
    gene_gene: pl.DataFrame,
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
                anatomy_gene.select(pl.col("to").alias("id")),
                biological_process_gene.select(pl.col("to").alias("id")),
                cellular_component_gene.select(pl.col("to").alias("id")),
                disease_gene.select(pl.col("to").alias("id")),
                drug_gene.select(pl.col("to").alias("id")),
                exposure_gene.select(pl.col("to").alias("id")),
                molecular_function_gene.select(pl.col("to").alias("id")),
                pathway_gene.select(pl.col("to").alias("id")),
                phenotype_gene.select(pl.col("to").alias("id")),
                gene_gene.select(pl.col("to").alias("id")),
                gene_gene.select(pl.col("from").alias("id")),
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
                                    pl.lit([Source.OPEN_TARGETS, Source.BGEE]).cast(
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
        "anatomy_gene": "silver.edges.anatomy_gene",
        "biological_process_gene": "silver.edges.biological_process_gene",
        "exposure_gene": "silver.edges.exposure_gene",
        "drug_gene": "silver.edges.drug_gene",
        "cellular_component_gene": "silver.edges.cellular_component_gene",
        "molecular_function_gene": "silver.edges.molecular_function_gene",
        "pathway_gene": "silver.edges.pathway_gene",
        "disease_gene": "silver.edges.disease_gene",
        "phenotype_gene": "silver.edges.phenotype_gene",
        "gene_expressions_in_anatomy": "bronze.bgee.gene_expressions_in_anatomy",
        "target": "bronze.opentargets.target",
        "gene_gene": "silver.edges.gene_gene",
    },
    outputs="nodes.gene",
    name="gene",
    tags=["silver"],
)
