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
from .edges.utils import normalize_edge_topology
from optimuskg.utils import format_rich

logger = logging.getLogger(__name__)


def export_to_biocypher(
    edge_datasets: list[pl.DataFrame],
    node_datasets: list[pl.DataFrame],
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
    for node_dataset in node_datasets:
        node_adapters.append(
            yield_nodes(
                df=node_dataset,
                mapping_config=node_config,
            )
        )

    edge_adapters = []
    for edge_dataset in edge_datasets:
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


def export_to_csv(
    edge_datasets: list[pl.DataFrame], node_datasets: list[pl.DataFrame]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    kg_edges = normalize_edge_topology(pl.concat(edge_datasets))
    kg_nodes = pl.concat(node_datasets).unique()

    return kg_edges, kg_nodes


def process_export_graph(  # noqa: PLR0913
    # Edges
    anatomy_protein_absent: pl.DataFrame,
    anatomy_protein_present: pl.DataFrame,
    biological_process_protein: pl.DataFrame,
    cellular_component_protein: pl.DataFrame,
    disease_protein_negative: pl.DataFrame,
    disease_protein_positive: pl.DataFrame,
    disease_protein: pl.DataFrame,
    disease_disease: pl.DataFrame,
    drug_drug: pl.DataFrame,
    drug_protein: pl.DataFrame,
    exposure_exposure: pl.DataFrame,
    exposure_protein: pl.DataFrame,
    exposure_disease: pl.DataFrame,
    indication: pl.DataFrame,
    contraindication: pl.DataFrame,
    off_label_use: pl.DataFrame,
    molecular_function_protein: pl.DataFrame,
    pathway_pathway: pl.DataFrame,
    pathway_protein: pl.DataFrame,
    phenotype_protein: pl.DataFrame,
    strong_clinical_evidence: pl.DataFrame,
    weak_clinical_evidence: pl.DataFrame,
    exposure_biological_process: pl.DataFrame,
    exposure_molecular_function: pl.DataFrame,
    exposure_cellular_component: pl.DataFrame,
    # Nodes
    gene: pl.DataFrame,
    anatomical_entity: pl.DataFrame,
    environmental_exposure: pl.DataFrame,
    drug: pl.DataFrame,
    disease: pl.DataFrame,
    phenotype: pl.DataFrame,
    biological_process: pl.DataFrame,
    cellular_component: pl.DataFrame,
    molecular_function: pl.DataFrame,
    pathway: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:

    node_datasets = [
        gene,
        anatomical_entity,
        environmental_exposure,
        drug,
        disease,
        phenotype,
        biological_process,
        cellular_component,
        molecular_function,
        pathway,
    ]
    edge_datasets = [
        anatomy_protein_absent,
        anatomy_protein_present,
        biological_process_protein,
        cellular_component_protein,
        disease_protein_negative,
        disease_protein_positive,
        disease_protein,
        disease_disease,
        drug_drug,
        drug_protein,
        exposure_exposure,
        exposure_protein,
        exposure_disease,
        indication,
        contraindication,
        off_label_use,
        molecular_function_protein,
        pathway_pathway,
        pathway_protein,
        phenotype_protein,
        strong_clinical_evidence,
        weak_clinical_evidence,
        exposure_biological_process,
        exposure_molecular_function,
        exposure_cellular_component,
        exposure_biological_process,
        exposure_molecular_function,
        exposure_cellular_component,
    ]

    export_to_biocypher(edge_datasets, node_datasets)
    kg_edges, kg_nodes = export_to_csv(edge_datasets, node_datasets)

    return kg_edges, kg_nodes


export_graph_node = node(
    process_export_graph,
    inputs={
        # Edges
        "anatomy_protein_absent": "gold.edges.anatomy_protein_absent",
        "anatomy_protein_present": "gold.edges.anatomy_protein_present",
        "biological_process_protein": "gold.edges.biological_process_protein",
        "cellular_component_protein": "gold.edges.cellular_component_protein",
        "disease_protein_negative": "gold.edges.disease_protein_negative",
        "disease_protein_positive": "gold.edges.disease_protein_positive",
        "disease_protein": "gold.edges.disease_protein",
        "disease_disease": "gold.edges.disease_disease",
        "drug_drug": "gold.edges.drug_drug",
        "drug_protein": "gold.edges.drug_protein",
        "exposure_exposure": "gold.edges.exposure_exposure",
        "exposure_protein": "gold.edges.exposure_protein",
        "exposure_disease": "gold.edges.exposure_disease",
        "indication": "gold.edges.indication",
        "contraindication": "gold.edges.contraindication",
        "off_label_use": "gold.edges.off_label_use",
        "molecular_function_protein": "gold.edges.molecular_function_protein",
        "pathway_pathway": "gold.edges.pathway_pathway",
        "pathway_protein": "gold.edges.pathway_protein",
        "phenotype_protein": "gold.edges.phenotype_protein",
        "strong_clinical_evidence": "gold.edges.strong_clinical_evidence",
        "weak_clinical_evidence": "gold.edges.weak_clinical_evidence",
        "exposure_biological_process": "gold.edges.exposure_biological_process",
        "exposure_molecular_function": "gold.edges.exposure_molecular_function",
        "exposure_cellular_component": "gold.edges.exposure_cellular_component",
        # Nodes
        "gene": "gold.nodes.gene",
        "anatomical_entity": "gold.nodes.anatomical_entity",
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
