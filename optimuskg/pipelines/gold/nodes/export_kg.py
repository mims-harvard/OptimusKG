import logging
import tempfile
from typing import Any

import networkx as nx
import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.gold.export_formats import (
    neo4j_export,
    parquet_export,
)
from optimuskg.pipelines.gold.utils.biocypher import run_biocypher

logger = logging.getLogger(__name__)

_EXPORT_FORMATS_DICT = {
    "parquet": parquet_export,
    "neo4j": neo4j_export,
}


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
    anatomy_gene: pl.DataFrame,
    biological_process_gene: pl.DataFrame,
    cellular_component_gene: pl.DataFrame,
    disease_gene: pl.DataFrame,
    disease_disease: pl.DataFrame,
    disease_phenotype: pl.DataFrame,
    drug_drug: pl.DataFrame,
    drug_gene: pl.DataFrame,
    drug_disease: pl.DataFrame,
    exposure_exposure: pl.DataFrame,
    exposure_gene: pl.DataFrame,
    exposure_disease: pl.DataFrame,
    molecular_function_gene: pl.DataFrame,
    pathway_pathway: pl.DataFrame,
    pathway_gene: pl.DataFrame,
    phenotype_gene: pl.DataFrame,
    gene_gene: pl.DataFrame,
    exposure_biological_process: pl.DataFrame,
    exposure_molecular_function: pl.DataFrame,
    exposure_cellular_component: pl.DataFrame,
    cellular_component_cellular_component: pl.DataFrame,
    biological_process_biological_process: pl.DataFrame,
    molecular_function_molecular_function: pl.DataFrame,
    phenotype_phenotype: pl.DataFrame,
    anatomy_anatomy: pl.DataFrame,
    drug_phenotype: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Export knowledge graph to various formats.

    Args:
        export_formats: Dictionary of format name to config.
            Each config should have an "include_properties" boolean key.
            Example: {"parquet": {"include_properties": True}}
        gene, anatomy, ...: Node DataFrames with schema (id, label, properties).
        anatomy_gene, ...: Edge DataFrames with schema (from, to, label, relation, undirected, properties).

    Returns:
        Parquet output dictionary with keys like "nodes", "edges",
        "nodes/gene", "edges/disease_gene", etc.
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
        "anatomy_gene": anatomy_gene,
        "biological_process_gene": biological_process_gene,
        "cellular_component_gene": cellular_component_gene,
        "disease_gene": disease_gene,
        "disease_disease": disease_disease,
        "disease_phenotype": disease_phenotype,
        "drug_drug": drug_drug,
        "drug_gene": drug_gene,
        "drug_disease": drug_disease,
        "exposure_exposure": exposure_exposure,
        "exposure_gene": exposure_gene,
        "exposure_disease": exposure_disease,
        "molecular_function_gene": molecular_function_gene,
        "pathway_pathway": pathway_pathway,
        "pathway_gene": pathway_gene,
        "phenotype_gene": phenotype_gene,
        "gene_gene": gene_gene,
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

    logger.info("Validating knowledge graph against BioCypher schema...")
    with tempfile.TemporaryDirectory(prefix="biocypher-validate-") as tmp_dir:
        try:
            run_biocypher(
                nodes_dict,
                edges_dict,
                include_properties=True,
                output_directory=tmp_dir,
            )
        except Exception:
            logger.exception("BioCypher schema validation failed")
            raise
    logger.info("BioCypher schema validation passed.")

    logger.info(
        f"Exporting knowledge graph to formats: {', '.join(export_formats.keys())}"
    )

    # Build graph to extract largest connected component
    G = nx.Graph()
    for df in nodes_dict.values():
        for nid, label in zip(df["id"].to_list(), df["label"].to_list()):
            G.add_node(nid, label=label)
    for df in edges_dict.values():
        if not df.is_empty():
            for src, dst, label in zip(
                df["from"].to_list(),
                df["to"].to_list(),
                df["label"].to_list(),
            ):
                G.add_edge(src, dst, label=label)

    lcc_ids = list(max(nx.connected_components(G), key=len))
    logger.info(
        f"Largest connected component: {len(lcc_ids)} of {G.number_of_nodes()} nodes"
    )

    parquet_output: dict[str, pl.DataFrame] = {}

    for format_name, config in export_formats.items():
        export_func = _EXPORT_FORMATS_DICT.get(format_name)
        if not export_func:
            logger.warning(f"Unknown export format: {format_name}")
            continue

        include_properties = config.get("include_properties", True)
        logger.info(
            f"Exporting to {format_name} (include_properties={include_properties})..."
        )

        if format_name == "parquet":
            fmt_output = export_func(nodes_dict, edges_dict, include_properties)

            fmt_output["largest_connected_component_nodes"] = fmt_output[
                "nodes"
            ].filter(pl.col("id").is_in(lcc_ids))
            fmt_output["largest_connected_component_edges"] = fmt_output[
                "edges"
            ].filter(pl.col("from").is_in(lcc_ids) & pl.col("to").is_in(lcc_ids))
            parquet_output = fmt_output
        elif format_name == "neo4j":
            export_func(nodes_dict, edges_dict, include_properties)

    return parquet_output


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
        "anatomy_gene": "silver.edges.anatomy_gene",
        "biological_process_gene": "silver.edges.biological_process_gene",
        "cellular_component_gene": "silver.edges.cellular_component_gene",
        "disease_gene": "silver.edges.disease_gene",
        "disease_disease": "silver.edges.disease_disease",
        "disease_phenotype": "silver.edges.disease_phenotype",
        "drug_drug": "silver.edges.drug_drug",
        "drug_gene": "silver.edges.drug_gene",
        "drug_disease": "silver.edges.drug_disease",
        "exposure_exposure": "silver.edges.exposure_exposure",
        "exposure_gene": "silver.edges.exposure_gene",
        "exposure_disease": "silver.edges.exposure_disease",
        "molecular_function_gene": "silver.edges.molecular_function_gene",
        "pathway_pathway": "silver.edges.pathway_pathway",
        "pathway_gene": "silver.edges.pathway_gene",
        "phenotype_gene": "silver.edges.phenotype_gene",
        "gene_gene": "silver.edges.gene_gene",
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
    outputs="kg.parquet",
    tags=["gold"],
    name="export_kg",
)
