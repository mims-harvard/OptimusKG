import logging

import polars as pl
from biocypher import BioCypher
from kedro.pipeline import node
from more_itertools import peekable

from optimuskg.pipelines.gold.adapter import adapter_factory
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

    node_adapters = [
        adapter_factory(
            gene_nodes,
            name="gene",
            node_configs=[
                NodeMappingConfig(
                    id_field="id",
                    label_field="type",
                    properties_fields=["name", "source"],
                ),
            ],
        ),
        adapter_factory(
            anatomical_entity_nodes,
            name="anatomical_entity",
            node_configs=[
                NodeMappingConfig(
                    id_field="id",
                    label_field="type",
                    properties_fields=["name", "source"],
                ),
            ],
        ),
        adapter_factory(
            environmental_exposure_nodes,
            name="environmental_exposure",
            node_configs=[
                NodeMappingConfig(
                    id_field="id",
                    label_field="type",
                    properties_fields=["name", "source"],
                ),
            ],
        ),
        adapter_factory(
            drug_nodes,
            name="drug",
            node_configs=[
                NodeMappingConfig(
                    id_field="id",
                    label_field="type",
                    properties_fields=["name", "source"],
                ),
            ],
        ),
        adapter_factory(
            disease_nodes,
            name="disease",
            node_configs=[
                NodeMappingConfig(
                    id_field="id",
                    label_field="type",
                    properties_fields=["name", "source"],
                ),
            ],
        ),
        adapter_factory(
            phenotype_nodes,
            name="phenotype",
            node_configs=[
                NodeMappingConfig(
                    id_field="id",
                    label_field="type",
                    properties_fields=["name", "source"],
                ),
            ],
        ),
        adapter_factory(
            biological_process_nodes,
            name="biological_process",
            node_configs=[
                NodeMappingConfig(
                    id_field="id",
                    label_field="type",
                    properties_fields=["name", "source"],
                ),
            ],
        ),
        adapter_factory(
            cellular_component_nodes,
            name="cellular_component",
            node_configs=[
                NodeMappingConfig(
                    id_field="id",
                    label_field="type",
                    properties_fields=["name", "source"],
                ),
            ],
        ),
        adapter_factory(
            molecular_function_nodes,
            name="molecular_function",
            node_configs=[
                NodeMappingConfig(
                    id_field="id",
                    label_field="type",
                    properties_fields=["name", "source"],
                ),
            ],
        ),
        adapter_factory(
            pathway_nodes,
            name="pathway",
            node_configs=[
                NodeMappingConfig(
                    id_field="id",
                    label_field="type",
                    properties_fields=["name", "source"],
                ),
            ],
        ),
    ]

    edge_adapters = [
        adapter_factory(
            gene_expressions_in_anatomy,
            name="bgee",
            edge_configs=[
                EdgeMappingConfig(
                    source_field="x_id",
                    target_field="y_id",
                    label_field="relation",
                    properties_fields=["display_relation"],
                )
            ],
        ),
        adapter_factory(
            ctd_exposure_protein_interactions,
            name="ctd_exposure_protein",
            edge_configs=[
                EdgeMappingConfig(
                    source_field="x_id",
                    target_field="y_id",
                    label_field="relation",
                    properties_fields=["display_relation"],
                )
            ],
        ),
        adapter_factory(
            ctd_exposure_exposure_interactions,
            name="ctd_exposure_exposure",
            edge_configs=[
                EdgeMappingConfig(
                    source_field="x_id",
                    target_field="y_id",
                    label_field="relation",
                    properties_fields=["display_relation"],
                )
            ],
        ),
        adapter_factory(
            drug_protein_interactions,
            name="drug_protein",
            edge_configs=[
                EdgeMappingConfig(
                    source_field="x_id",
                    target_field="y_id",
                    label_field="relation",
                    properties_fields=["display_relation"],
                )
            ],
        ),
        adapter_factory(
            drug_drug_interactions,
            name="drug_drug",
            edge_configs=[
                EdgeMappingConfig(
                    source_field="x_id",
                    target_field="y_id",
                    label_field="relation",
                    properties_fields=["display_relation"],
                )
            ],
        ),
        adapter_factory(
            opentargets_edges,
            name="opentargets",
            edge_configs=[
                EdgeMappingConfig(
                    source_field="x_id",
                    target_field="y_id",
                    label_field="relation",
                    properties_fields=["display_relation"],
                )
            ],
        ),
        adapter_factory(
            protein_biological_process_interactions,
            name="ncbigene_protein_biological_process",
            edge_configs=[
                EdgeMappingConfig(
                    source_field="x_id",
                    target_field="y_id",
                    label_field="relation",
                    properties_fields=["display_relation"],
                )
            ],
        ),
        adapter_factory(
            protein_cellular_component_interactions,
            name="ncbigene_protein_cellular_component",
            edge_configs=[
                EdgeMappingConfig(
                    source_field="x_id",
                    target_field="y_id",
                    label_field="relation",
                    properties_fields=["display_relation"],
                )
            ],
        ),
        adapter_factory(
            protein_molecular_function_interactions,
            name="ncbigene_protein_molecular_function",
            edge_configs=[
                EdgeMappingConfig(
                    source_field="x_id",
                    target_field="y_id",
                    label_field="relation",
                    properties_fields=["display_relation"],
                )
            ],
        ),
        adapter_factory(
            pathway_pathway_interactions,
            name="reactome_pathway_pathway",
            edge_configs=[
                EdgeMappingConfig(
                    source_field="x_id",
                    target_field="y_id",
                    label_field="relation",
                    properties_fields=["display_relation"],
                )
            ],
        ),
        adapter_factory(
            pathway_protein_interactions,
            name="reactome_pathway_protein",
            edge_configs=[
                EdgeMappingConfig(
                    source_field="x_id",
                    target_field="y_id",
                    label_field="relation",
                    properties_fields=["display_relation"],
                )
            ],
        ),
    ]

    # TODO: Add adapters for other datasets

    adapters = [
        *node_adapters,
        *edge_adapters,
    ]

    try:
        if not adapters:
            logger.warning("No adapters configured for processing.")
        else:
            for i, adapter in enumerate(adapters):
                logger.info(
                    f"Processing {format_rich(str(i + 1), 'dark_orange')} out of {format_rich(str(len(adapters)), 'dark_orange')} adapters"
                )
                if (nodes_p := peekable(adapter.nodes())).peek(None) is not None:
                    logger.info(
                        f"Writing nodes for {format_rich(adapter.name, 'dark_orange')}..."
                    )
                    bc.write_nodes(nodes_p)
                else:
                    logger.warning(
                        f"No nodes to write for {format_rich(adapter.name, 'dark_orange')}."
                    )

                if (edges_p := peekable(adapter.edges())).peek(None) is not None:
                    logger.info(
                        f"Writing edges for {format_rich(adapter.name, 'dark_orange')}..."
                    )
                    bc.write_edges(edges_p)
                else:
                    logger.warning(
                        f"No edges to write for {format_rich(adapter.name, 'dark_orange')}."
                    )
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
