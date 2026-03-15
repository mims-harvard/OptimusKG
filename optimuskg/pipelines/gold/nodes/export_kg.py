import logging
from typing import Any

import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.gold.export_formats import (
    csv_export,
    neo4j_export,
    parquet_export,
)
from optimuskg.pipelines.silver.nodes.constants import Node

logger = logging.getLogger(__name__)

_EXPORT_FORMATS_DICT = {
    "csv": csv_export,
    "parquet": parquet_export,
    "neo4j": neo4j_export,
}


def _namespace_node_ids(
    nodes_dict: dict[str, pl.DataFrame],
) -> dict[str, pl.DataFrame]:
    """Prefix node IDs with their label to ensure global uniqueness.

    Each node's ``id`` becomes ``"{label}:{original_id}"``,
    e.g. ``"GEN:ENSG00000141510"``.
    """
    return {
        name: df.with_columns(
            (pl.col("label") + pl.lit(":") + pl.col("id")).alias("id")
        )
        for name, df in nodes_dict.items()
    }


def _namespace_edge_ids(
    edges_dict: dict[str, pl.DataFrame],
) -> dict[str, pl.DataFrame]:
    """Prefix edge ``from``/``to`` IDs with their node-type label.

    The node type is derived from the edge's ``label`` column
    (e.g. ``"DIS-PHE"``).  Proteins (``PRO``) are mapped to genes
    (``GEN``) because protein nodes are stored as gene nodes.
    """
    result: dict[str, pl.DataFrame] = {}
    for name, df in edges_dict.items():
        if df.is_empty():
            result[name] = df
            continue

        edge_label: str = df.select("label").row(0)[0]
        from_type, to_type = edge_label.split("-")

        # Proteins are represented as gene nodes in the KG.
        from_ns = Node.GENE if from_type == Node.PROTEIN else from_type
        to_ns = Node.GENE if to_type == Node.PROTEIN else to_type

        result[name] = df.with_columns(
            (pl.lit(f"{from_ns}:") + pl.col("from")).alias("from"),
            (pl.lit(f"{to_ns}:") + pl.col("to")).alias("to"),
        )
    return result


def _validate_global_id_uniqueness(
    nodes_dict: dict[str, pl.DataFrame],
) -> None:
    """Validate that node IDs are globally unique across all node types.

    Raises:
        ValueError: If duplicate IDs are found across node types.
    """
    non_empty = [df.select("id") for df in nodes_dict.values() if not df.is_empty()]
    if not non_empty:
        return

    all_ids = pl.concat(non_empty)
    duplicates = all_ids.filter(pl.col("id").is_duplicated()).unique()

    if duplicates.height > 0:
        sample = duplicates.head(10).to_series().to_list()
        raise ValueError(
            f"Found {duplicates.height} non-unique node IDs across node types. "
            f"Sample: {sample}"
        )

    logger.info(
        f"Validated global ID uniqueness: {all_ids.height} IDs are globally unique."
    )


def export_kg(  # noqa: PLR0913
    export_formats: dict[str, dict[str, Any]],
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
) -> tuple[dict[str, pl.DataFrame], dict[str, pl.DataFrame]]:
    """Export knowledge graph to various formats.

    Args:
        export_formats: Dictionary of format name to config.
            Each config should have an "include_properties" boolean key.
            Example: {"csv": {"include_properties": True}, "parquet": {"include_properties": False}}
        gene, anatomy, ...: Node DataFrames with schema (id, label, properties).
        anatomy_protein, ...: Edge DataFrames with schema (from, to, label, relation, undirected, properties).

    Returns:
        Tuple of (csv_output, parquet_output) dictionaries.
        Each dictionary contains keys like "nodes", "edges", "nodes/gene", "edges/disease_protein", etc.
    """
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

    # Namespace IDs for global uniqueness and validate.
    nodes_dict = _namespace_node_ids(nodes_dict)
    edges_dict = _namespace_edge_ids(edges_dict)
    _validate_global_id_uniqueness(nodes_dict)

    logger.info(
        f"Exporting knowledge graph to formats: {', '.join(export_formats.keys())}"
    )

    outputs: dict[str, dict[str, pl.DataFrame]] = {
        "kg.csv": {},
        "kg.parquet": {},
    }

    for format_name, config in export_formats.items():
        export_func = _EXPORT_FORMATS_DICT.get(format_name)
        if not export_func:
            logger.warning(f"Unknown export format: {format_name}")
            continue

        include_properties = config.get("include_properties", True)
        logger.info(
            f"Exporting to {format_name} (include_properties={include_properties})..."
        )

        if format_name in ["csv", "parquet"]:
            outputs[f"kg.{format_name}"] = export_func(
                nodes_dict, edges_dict, include_properties
            )
        elif format_name == "neo4j":
            export_func(nodes_dict, edges_dict, include_properties)

    return (
        outputs["kg.csv"],
        outputs["kg.parquet"],
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
    outputs=["kg.csv", "kg.parquet"],
    tags=["gold"],
    name="export_kg",
)
