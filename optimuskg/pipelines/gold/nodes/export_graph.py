import logging

import polars as pl
from biocypher import BioCypher
from kedro.pipeline import node
from more_itertools import peekable

from optimuskg.pipelines.gold.adapter import yield_edges, yield_nodes
from optimuskg.pipelines.gold.adapter.mapping import (
    EdgeMappingConfig,
    NodeMappingConfig,
)
from optimuskg.utils import format_rich

from .edges.utils import normalize_edge_topology

logger = logging.getLogger(__name__)


def _export_to_biocypher(  # noqa: PLR0912
    edges: list[pl.DataFrame],
    nodes: list[pl.DataFrame],
) -> None:
    bc = BioCypher(
        head_ontology={
            "url": "https://github.com/biolink/biolink-model/raw/v3.2.1/biolink-model.owl.ttl",
            "root_node": "entity",
            "format": "ttl",
        },
        biocypher_config_path="conf/base/biocypher/biocypher_config.yaml",
    )

    node_config = NodeMappingConfig(
        id_field="id",
        label_field="type",
        properties_fields=["name", "source"],
    )

    edge_config = EdgeMappingConfig(
        source_field="x_id",
        target_field="y_id",
        label_field="relation",
        properties_fields=["relation_type", "undirected"],
    )

    node_adapters = []
    for node_dataset in nodes:
        node_adapters.append(
            yield_nodes(
                df=node_dataset,
                mapping_config=node_config,
            )
        )

    edge_adapters = []
    for edge_dataset in edges:
        edge_adapters.append(
            yield_edges(
                df=edge_dataset,
                mapping_config=edge_config,
            )
        )

    try:
        if not node_adapters:
            logger.error("There are no nodes to process.")
        else:
            logger.info(
                f"Starting node processing using {format_rich(str(len(node_adapters)), 'dark_orange')} adapter(s)."
            )
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

        # Process Edges
        if not edge_adapters:
            logger.warning("No edge adapters configured. Skipping edge processing.")
        else:
            logger.info(
                f"Starting edge processing using {format_rich(str(len(edge_adapters)), 'dark_orange')} adapter(s)."
            )
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


def _export_to_csv(
    edges: list[pl.DataFrame], nodes: list[pl.DataFrame]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    kg_edges = normalize_edge_topology(pl.concat(edges))
    kg_nodes = pl.concat(nodes).unique()

    return kg_edges, kg_nodes


def run(  # noqa: PLR0913
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
    # Nodes
    gene: pl.DataFrame,
    anatomy: pl.DataFrame,
    environmental_exposure: pl.DataFrame,
    drug: pl.DataFrame,
    disease: pl.DataFrame,
    phenotype: pl.DataFrame,
    biological_process: pl.DataFrame,
    cellular_component: pl.DataFrame,
    molecular_function: pl.DataFrame,
    pathway: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    nodes = [
        gene,
        anatomy,
        environmental_exposure,
        drug,
        disease,
        phenotype,
        biological_process,
        cellular_component,
        molecular_function,
        pathway,
    ]
    edges = [
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
    ]

    _export_to_biocypher(edges, nodes)
    kg_edges, kg_nodes = _export_to_csv(edges, nodes)

    return kg_edges, kg_nodes


export_graph_node = node(
    run,
    inputs={
        # Edges
        "anatomy_protein": "gold.edges.anatomy_protein",
        "biological_process_protein": "gold.edges.biological_process_protein",
        "cellular_component_protein": "gold.edges.cellular_component_protein",
        "disease_protein": "gold.edges.disease_protein",
        "disease_disease": "gold.edges.disease_disease",
        "disease_phenotype": "gold.edges.disease_phenotype",
        "drug_drug": "gold.edges.drug_drug",
        "drug_protein": "gold.edges.drug_protein",
        "drug_disease": "gold.edges.drug_disease",
        "exposure_exposure": "gold.edges.exposure_exposure",
        "exposure_protein": "gold.edges.exposure_protein",
        "exposure_disease": "gold.edges.exposure_disease",
        "molecular_function_protein": "gold.edges.molecular_function_protein",
        "pathway_pathway": "gold.edges.pathway_pathway",
        "pathway_protein": "gold.edges.pathway_protein",
        "phenotype_protein": "gold.edges.phenotype_protein",
        "exposure_biological_process": "gold.edges.exposure_biological_process",
        "exposure_molecular_function": "gold.edges.exposure_molecular_function",
        "exposure_cellular_component": "gold.edges.exposure_cellular_component",
        "cellular_component_cellular_component": "gold.edges.cellular_component_cellular_component",
        "biological_process_biological_process": "gold.edges.biological_process_biological_process",
        "molecular_function_molecular_function": "gold.edges.molecular_function_molecular_function",
        "phenotype_phenotype": "gold.edges.phenotype_phenotype",
        "anatomy_anatomy": "gold.edges.anatomy_anatomy",
        # Nodes
        "gene": "gold.nodes.gene",
        "anatomy": "gold.nodes.anatomy",
        "environmental_exposure": "gold.nodes.environmental_exposure",
        "drug": "gold.nodes.drug",
        "disease": "gold.nodes.disease",
        "phenotype": "gold.nodes.phenotype",
        "biological_process": "gold.nodes.biological_process",
        "cellular_component": "gold.nodes.cellular_component",
        "molecular_function": "gold.nodes.molecular_function",
        "pathway": "gold.nodes.pathway",
    },
    outputs=["optimuskg_edges", "optimuskg_nodes"],
    tags=["gold"],
    name="export_graph",
)
