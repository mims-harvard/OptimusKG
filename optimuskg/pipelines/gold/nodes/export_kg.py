import logging

import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.gold.export_formats import (
    csv_export,
    neo4j_export,
    parquet_export,
    pg_export,
)

logger = logging.getLogger(__name__)

_EXPORT_FORMATS_DICT = {
    "csv": csv_export,
    "parquet": parquet_export,
    "pg": pg_export,
    "neo4j": neo4j_export,
}


def export_kg(  # noqa: PLR0913
    export_formats: list[str],
    # Nodes
    gene: pl.DataFrame,
    anatomy: pl.DataFrame,
    exposure: pl.DataFrame,
    drug: pl.DataFrame,
    disease: pl.DataFrame,
    phenotype: pl.DataFrame,
    biological_process: pl.DataFrame,
    cellular_component: pl.DataFrame,
    molecular_function: pl.DataFrame,
    pathway: pl.DataFrame,
    # Edges
    anatomy_protein: pl.DataFrame,
    biological_process_protein: pl.DataFrame,
    cellular_component_protein: pl.DataFrame,
    disease_protein: pl.DataFrame,
    disease_disease: pl.DataFrame,
    disease_phenotype: pl.DataFrame,
    drug_drug: pl.DataFrame,
    drug_protein: pl.DataFrame,
    drug_disease: pl.DataFrame,
    exposure_exposure: pl.DataFrame,
    exposure_protein: pl.DataFrame,
    exposure_disease: pl.DataFrame,
    molecular_function_protein: pl.DataFrame,
    pathway_pathway: pl.DataFrame,
    pathway_protein: pl.DataFrame,
    phenotype_protein: pl.DataFrame,
    protein_protein: pl.DataFrame,
    exposure_biological_process: pl.DataFrame,
    exposure_molecular_function: pl.DataFrame,
    exposure_cellular_component: pl.DataFrame,
    cellular_component_cellular_component: pl.DataFrame,
    biological_process_biological_process: pl.DataFrame,
    molecular_function_molecular_function: pl.DataFrame,
    phenotype_phenotype: pl.DataFrame,
    anatomy_anatomy: pl.DataFrame,
    drug_phenotype: pl.DataFrame,
) -> tuple[
    dict[str, pl.DataFrame],
    dict[str, pl.DataFrame],
    dict[str, pl.DataFrame],
    dict[str, pl.DataFrame],
]:
    nodes_dict = {
        "gene": gene,
        "anatomy": anatomy,
        "exposure": exposure,
        "drug": drug,
        "disease": disease,
        "phenotype": phenotype,
        "biological_process": biological_process,
        "cellular_component": cellular_component,
        "molecular_function": molecular_function,
        "pathway": pathway,
    }

    edges_dict = {
        "anatomy_protein": anatomy_protein,
        "biological_process_protein": biological_process_protein,
        "cellular_component_protein": cellular_component_protein,
        "disease_protein": disease_protein,
        "disease_disease": disease_disease,
        "disease_phenotype": disease_phenotype,
        "drug_drug": drug_drug,
        "drug_protein": drug_protein,
        "drug_disease": drug_disease,
        "exposure_exposure": exposure_exposure,
        "exposure_protein": exposure_protein,
        "exposure_disease": exposure_disease,
        "molecular_function_protein": molecular_function_protein,
        "pathway_pathway": pathway_pathway,
        "pathway_protein": pathway_protein,
        "phenotype_protein": phenotype_protein,
        "protein_protein": protein_protein,
        "exposure_biological_process": exposure_biological_process,
        "exposure_molecular_function": exposure_molecular_function,
        "exposure_cellular_component": exposure_cellular_component,
        "cellular_component_cellular_component": cellular_component_cellular_component,
        "biological_process_biological_process": biological_process_biological_process,
        "molecular_function_molecular_function": molecular_function_molecular_function,
        "phenotype_phenotype": phenotype_phenotype,
        "anatomy_anatomy": anatomy_anatomy,
        "drug_phenotype": drug_phenotype,
    }

    logger.info(f"Exporting knowledge graph to formats: {', '.join(export_formats)}")

    outputs = {
        "csv_meta": {},
        "csv_no_meta": {},
        "parquet_meta": {},
        "parquet_no_meta": {},
    }

    for format_name in export_formats:
        export_func = _EXPORT_FORMATS_DICT.get(format_name)
        if not export_func:
            logger.warning(f"Unknown export format: {format_name}")
            continue

        logger.info(f"Exporting to {format_name}...")
        if format_name in ["csv", "parquet"]:
            meta, no_meta = export_func(nodes_dict, edges_dict)
            outputs[f"{format_name}_meta"] = meta
            outputs[f"{format_name}_no_meta"] = no_meta
        else:
            export_func(nodes_dict, edges_dict)

    return (
        outputs["csv_meta"],
        outputs["csv_no_meta"],
        outputs["parquet_meta"],
        outputs["parquet_no_meta"],
    )


export_kg_node = node(
    export_kg,
    inputs={
        "export_formats": "params:export_formats",
        # Nodes
        "gene": "silver.nodes.gene",
        "anatomy": "silver.nodes.anatomy",
        "exposure": "silver.nodes.exposure",
        "drug": "silver.nodes.drug",
        "disease": "silver.nodes.disease",
        "phenotype": "silver.nodes.phenotype",
        "biological_process": "silver.nodes.biological_process",
        "cellular_component": "silver.nodes.cellular_component",
        "molecular_function": "silver.nodes.molecular_function",
        "pathway": "silver.nodes.pathway",
        # Edges
        "anatomy_protein": "silver.edges.anatomy_protein",
        "biological_process_protein": "silver.edges.biological_process_protein",
        "cellular_component_protein": "silver.edges.cellular_component_protein",
        "disease_protein": "silver.edges.disease_protein",
        "disease_disease": "silver.edges.disease_disease",
        "disease_phenotype": "silver.edges.disease_phenotype",
        "drug_drug": "silver.edges.drug_drug",
        "drug_protein": "silver.edges.drug_protein",
        "drug_disease": "silver.edges.drug_disease",
        "exposure_exposure": "silver.edges.exposure_exposure",
        "exposure_protein": "silver.edges.exposure_protein",
        "exposure_disease": "silver.edges.exposure_disease",
        "molecular_function_protein": "silver.edges.molecular_function_protein",
        "pathway_pathway": "silver.edges.pathway_pathway",
        "pathway_protein": "silver.edges.pathway_protein",
        "phenotype_protein": "silver.edges.phenotype_protein",
        "protein_protein": "silver.edges.protein_protein",
        "exposure_biological_process": "silver.edges.exposure_biological_process",
        "exposure_molecular_function": "silver.edges.exposure_molecular_function",
        "exposure_cellular_component": "silver.edges.exposure_cellular_component",
        "cellular_component_cellular_component": "silver.edges.cellular_component_cellular_component",
        "biological_process_biological_process": "silver.edges.biological_process_biological_process",
        "molecular_function_molecular_function": "silver.edges.molecular_function_molecular_function",
        "phenotype_phenotype": "silver.edges.phenotype_phenotype",
        "anatomy_anatomy": "silver.edges.anatomy_anatomy",
        "drug_phenotype": "silver.edges.drug_phenotype",
    },
    outputs=[
        "csv.meta",
        "csv.no_meta",
        "parquet.meta",
        "parquet.no_meta",
    ],
    tags=["gold"],
    name="export_kg",
)
