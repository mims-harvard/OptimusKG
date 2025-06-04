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
    gene_expressions_in_anatomy: pl.DataFrame,
    opentargets_edges: pl.DataFrame,
    ctd_exposure_protein_interactions: pl.DataFrame,
    ctd_exposure_exposure_interactions: pl.DataFrame,
    drug_protein_interactions: pl.DataFrame,
    drug_drug_interactions: pl.DataFrame,
    protein_biological_process_interactions: pl.DataFrame,
    protein_cellular_component_interactions: pl.DataFrame,
    protein_molecular_function_interactions: pl.DataFrame,
    pathway_pathway_interactions: pl.DataFrame,
    pathway_protein_interactions: pl.DataFrame,
    # Nodes
    gene_nodes: pl.DataFrame,
    anatomical_entity_nodes: pl.DataFrame,
    environmental_exposure_nodes: pl.DataFrame,
    drug_nodes: pl.DataFrame,
    disease_nodes: pl.DataFrame,
    phenotype_nodes: pl.DataFrame,
    biological_process_nodes: pl.DataFrame,
    cellular_component_nodes: pl.DataFrame,
    molecular_function_nodes: pl.DataFrame,
    pathway_nodes: pl.DataFrame,
) -> pl.DataFrame:
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
        properties_fields=["display_relation"],
    )

    node_adapters = [
        yield_nodes(
            df=gene_nodes,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=anatomical_entity_nodes,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=environmental_exposure_nodes,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=drug_nodes,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=disease_nodes,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=phenotype_nodes,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=biological_process_nodes,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=cellular_component_nodes,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=molecular_function_nodes,
            mapping_config=node_config,
        ),
        yield_nodes(
            df=pathway_nodes,
            mapping_config=node_config,
        ),
    ]

    edge_adapters = [
        yield_edges(
            df=gene_expressions_in_anatomy,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=opentargets_edges,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=ctd_exposure_protein_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=ctd_exposure_exposure_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=drug_protein_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=ctd_exposure_protein_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=ctd_exposure_exposure_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=drug_protein_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=drug_drug_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=drug_protein_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=drug_drug_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=drug_drug_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=protein_biological_process_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=protein_cellular_component_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=protein_molecular_function_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=pathway_pathway_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=pathway_protein_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=protein_cellular_component_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=protein_molecular_function_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=pathway_pathway_interactions,
            mapping_config=edge_config,
        ),
        yield_edges(
            df=pathway_protein_interactions,
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

    return pl.DataFrame()


biocypher_node = node(
    process_biocypher,
    inputs={
        "gene_expressions_in_anatomy": "silver.bgee.gene_expressions_in_anatomy",
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "ctd_exposure_protein_interactions": "silver.ctd.ctd_exposure_protein_interactions",
        "ctd_exposure_exposure_interactions": "silver.ctd.ctd_exposure_exposure_interactions",
        "drug_protein_interactions": "silver.drugbank.drug_protein",
        "drug_drug_interactions": "silver.drugbank.drug_drug",
        "protein_biological_process_interactions": "silver.ncbigene.protein_biological_process_interactions",
        "protein_cellular_component_interactions": "silver.ncbigene.protein_cellular_component_interactions",
        "protein_molecular_function_interactions": "silver.ncbigene.protein_molecular_function_interactions",
        "pathway_pathway_interactions": "silver.reactome.pathway_pathway_interactions",
        "pathway_protein_interactions": "silver.reactome.pathway_protein_interactions",
        # Nodes
        "gene_nodes": "gold.nodes.gene",
        "anatomical_entity_nodes": "gold.nodes.anatomical_entity",
        "environmental_exposure_nodes": "gold.nodes.environmental_exposure",
        "drug_nodes": "gold.nodes.drug",
        "disease_nodes": "gold.nodes.disease",
        "phenotype_nodes": "gold.nodes.phenotype",
        "biological_process_nodes": "gold.nodes.biological_process",
        "cellular_component_nodes": "gold.nodes.cellular_component",
        "molecular_function_nodes": "gold.nodes.molecular_function",
        "pathway_nodes": "gold.nodes.pathway",
    },
    outputs="biocypher_graph",
    tags=["gold"],
    name="biocypher",
)
