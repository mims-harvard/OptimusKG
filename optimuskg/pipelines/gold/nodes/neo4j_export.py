import logging

import polars as pl
from biocypher import BioCypher
from kedro.pipeline import node
from more_itertools import peekable

from optimuskg.utils import format_rich

from .utils import yield_edges, yield_nodes

logger = logging.getLogger(__name__)


def run(  # noqa: PLR0913
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
    exposure_biological_process: pl.DataFrame,
    exposure_molecular_function: pl.DataFrame,
    exposure_cellular_component: pl.DataFrame,
    cellular_component_cellular_component: pl.DataFrame,
    biological_process_biological_process: pl.DataFrame,
    molecular_function_molecular_function: pl.DataFrame,
    phenotype_phenotype: pl.DataFrame,
    anatomy_anatomy: pl.DataFrame,
    drug_phenotype: pl.DataFrame,
) -> None:
    bc = BioCypher(biocypher_config_path="conf/base/biocypher/biocypher_config.yaml")

    node_adapters = [
        yield_nodes(df)
        for df in [
            gene,
            anatomy,
            exposure,
            drug,
            disease,
            phenotype,
            biological_process,
            cellular_component,
            molecular_function,
            pathway,
        ]
    ]

    edge_adapters = [
        yield_edges(df)
        for df in [
            anatomy_protein,
            biological_process_protein,
            cellular_component_protein,
            disease_protein,
            disease_disease,
            disease_phenotype,
            drug_drug,
            drug_protein,
            drug_disease,
            exposure_exposure,
            exposure_protein,
            exposure_disease,
            molecular_function_protein,
            pathway_pathway,
            pathway_protein,
            phenotype_protein,
            exposure_biological_process,
            exposure_molecular_function,
            exposure_cellular_component,
            cellular_component_cellular_component,
            biological_process_biological_process,
            molecular_function_molecular_function,
            phenotype_phenotype,
            anatomy_anatomy,
            drug_phenotype,
        ]
    ]

    try:
        if not node_adapters:
            logger.error("There are no nodes to process.")
        else:
            for i, nodes_iterable in enumerate(node_adapters):
                logger.info(
                    f"Processing node adapter {format_rich(str(i + 1), 'dark_orange')}/{format_rich(str(len(node_adapters)), 'dark_orange')}."
                )

                nodes_p = peekable(nodes_iterable)
                if nodes_p.peek(None) is not None:
                    logger.info("Writing nodes...")
                    bc.write_nodes(nodes_p)
                else:
                    logger.warning("No nodes found.")

        if not edge_adapters:
            logger.error("There are no edges to process.")
        else:
            for i, edges_iterable in enumerate(edge_adapters):
                logger.info(
                    f"Processing edge adapter {format_rich(str(i + 1), 'dark_orange')}/{format_rich(str(len(edge_adapters)), 'dark_orange')}."
                )

                edges_p = peekable(edges_iterable)
                if edges_p.peek(None) is not None:
                    logger.info("Writing edges...")
                    bc.write_edges(edges_p)
                else:
                    logger.warning("No edges found.")
    except Exception as e:
        logger.exception(f"Error writing graph data to disk: {e}")
        raise


neo4j_export_node = node(
    run,
    inputs={
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
    outputs=None,
    tags=["gold"],
    name="neo4j_export",
)
