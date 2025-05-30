import logging

import polars as pl
from biocypher import BioCypher
from kedro.pipeline import node
from more_itertools import peekable

from optimuskg.pipelines.gold.adapter import adapter_factory
from optimuskg.utils import format_rich

logger = logging.getLogger(__name__)


def process_biocypher(  # noqa: PLR0913
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
) -> pl.DataFrame:
    bc = BioCypher(
        head_ontology={
            "url": "https://github.com/biolink/biolink-model/raw/v3.2.1/biolink-model.owl.ttl",
            "root_node": "entity",
            "format": "ttl",
        },
        biocypher_config_path="conf/base/biocypher/biocypher_config.yaml",
    )

    # Define individual adapter instances
    bgee_adapter = adapter_factory(gene_expressions_in_anatomy, name="bgee")
    ctd_adapters = [
        adapter_factory(ctd_exposure_protein_interactions, name="ctd_exposure_protein"),
        adapter_factory(
            ctd_exposure_exposure_interactions, name="ctd_exposure_exposure"
        ),
    ]
    opentargets_adapter = adapter_factory(opentargets_edges, name="opentargets")
    drugbank_adapters = [
        adapter_factory(drug_protein_interactions, name="drugbank_drug_protein"),
        adapter_factory(drug_drug_interactions, name="drugbank_drug_drug"),
    ]
    ncbigene_adapters = [
        adapter_factory(
            protein_biological_process_interactions,
            name="ncbigene_protein_biological_process",
        ),
        adapter_factory(
            protein_cellular_component_interactions,
            name="ncbigene_protein_cellular_component",
        ),
        adapter_factory(
            protein_molecular_function_interactions,
            name="ncbigene_protein_molecular_function",
        ),
    ]
    reactome_adapters = [
        adapter_factory(pathway_pathway_interactions, name="reactome_pathway_pathway"),
        adapter_factory(pathway_protein_interactions, name="reactome_pathway_protein"),
    ]

    # TODO: Add adapters for other datasets

    adapters = [
        bgee_adapter,
        *ctd_adapters,
        opentargets_adapter,
        *drugbank_adapters,
        *ncbigene_adapters,
        *reactome_adapters,
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
    },
    outputs="biocypher_graph",
    tags=["gold"],
    name="biocypher",
)
