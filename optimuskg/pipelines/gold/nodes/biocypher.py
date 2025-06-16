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

logger = logging.getLogger(__name__)


def process_biocypher(  # noqa: PLR0913
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

    node_adapters = [
        yield_nodes(
            df=gene,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=anatomical_entity,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=environmental_exposure,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=drug,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=disease,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=phenotype,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=biological_process,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=cellular_component,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=molecular_function,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=pathway,
            mapping_config=node_config,
        ),
    ]

    edge_adapters = [
        yield_edges(
            df=anatomy_protein_absent,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=anatomy_protein_present,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=biological_process_protein,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=cellular_component_protein,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=disease_protein_negative,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=disease_protein_positive,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=disease_protein,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=disease_disease,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=drug_drug,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=drug_protein,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=exposure_exposure,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=exposure_protein,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=exposure_disease,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=indication,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=contraindication,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=off_label_use,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=molecular_function_protein,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=pathway_pathway,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=pathway_protein,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=phenotype_protein,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=strong_clinical_evidence,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=weak_clinical_evidence,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=exposure_biological_process,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=exposure_molecular_function,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=exposure_cellular_component,
            mapping_config=edge_config,
        ),
    ]

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


biocypher_node = node(
    process_biocypher,
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
    outputs=None,
    tags=["gold"],
    name="biocypher",
)
