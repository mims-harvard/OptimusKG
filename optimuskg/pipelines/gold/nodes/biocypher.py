import logging

import polars as pl
from biocypher import BioCypher
from kedro.pipeline import node

from optimuskg.datasets.owl_dataset import LoadedOWLDataset
from optimuskg.pipelines.gold.adapter import adapter_factory

logger = logging.getLogger(__name__)


def process_biocypher(  # noqa: PLR0913
    # Ontologies
    biolink_ontology: LoadedOWLDataset,
    disease_ontology: LoadedOWLDataset,
    # gene_ontology: LoadedOWLDataset,
    human_phenotype_ontology: LoadedOWLDataset,
    mondo_ontology: LoadedOWLDataset,
    orphanet_ontology: LoadedOWLDataset,
    uberon_ontology: LoadedOWLDataset,
    # Data
    gene_expressions_in_anatomy: pl.DataFrame,
    opentargets_edges: pl.DataFrame,
    ctd_exposure_protein_interactions: pl.DataFrame,
    ctd_exposure_exposure_interactions: pl.DataFrame,
    drug_drug_interactions: pl.DataFrame,
    drug_protein_interactions: pl.DataFrame,
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
        tail_ontologies={
            # "doid": {
            #     "url": disease_ontology.filepath,
            #     "head_join_node": "disease",
            #     "tail_join_node": "disease",
            #     "merge_nodes": False,
            # },
            # "hpo": {
            #     "url": human_phenotype_ontology.filepath,
            #     "head_join_node": "disease",
            #     "tail_join_node": "human disease",
            #     "merge_nodes": False,
            # },
            # "mondo": { # NOTE: Mondo is mapping OK
            #     "url": mondo_ontology.filepath,
            #     "head_join_node": "disease",
            #     "tail_join_node": "human disease",
            #     "merge_nodes": False,
            # },
            # "ordo": {
            #     "url": orphanet_ontology.filepath,
            #     "head_join_node": "disease",
            #     "tail_join_node": "human disease",
            #     "merge_nodes": False,
            # },
        },
    )

    bgee_adapter = adapter_factory(gene_expressions_in_anatomy)
    opentargets_adapter = adapter_factory(opentargets_edges)
    # ctd_adapters = [
    #     adapter_factory(ctd_exposure_protein_interactions),
    #     adapter_factory(ctd_exposure_exposure_interactions),
    # ]
    # drugbank_adapters = [
    #     adapter_factory(drug_drug_interactions),
    #     adapter_factory(drug_protein_interactions),
    # ]
    # ncbigene_adapters = [
    #     adapter_factory(protein_biological_process_interactions),
    #     adapter_factory(protein_cellular_component_interactions),
    #     adapter_factory(protein_molecular_function_interactions),
    # ]
    # reactome_adapters = [
    #     adapter_factory(pathway_pathway_interactions),
    #     adapter_factory(pathway_protein_interactions),
    # ]

    # TODO: Add adapters for other datasets (DrugCental, Opentargets)

    adapters = [
        bgee_adapter,
        opentargets_adapter,
        # *ctd_adapters,
        # *drugbank_adapters,
        # *ncbigene_adapters,
        # *reactome_adapters,
    ]

    try:
        for i, adapter in enumerate(adapters):
            logger.debug(f"Writing {adapter.name} nodes")
            bc.write_nodes(adapter.nodes())
            logger.debug(f"Writing {adapter.name} edges")
            bc.write_edges(adapter.edges())
            logger.debug(f"Finished writing {adapter.name}")
        bc.write_import_call()
    except Exception as e:
        logger.exception(f"Error writing graph data to disk: {e}")
        raise

    return pl.DataFrame()


biocypher_node = node(
    process_biocypher,
    inputs={
        # Ontologies
        "biolink_ontology": "landing.ontology.biolink",
        "disease_ontology": "landing.ontology.disease",
        # "gene_ontology": "landing.ontology.gene",
        "human_phenotype_ontology": "landing.ontology.human_phenotype",
        "mondo_ontology": "landing.ontology.mondo",
        "orphanet_ontology": "landing.ontology.orphanet",
        "uberon_ontology": "landing.ontology.uber_anatomy",
        # Adapters
        "gene_expressions_in_anatomy": "silver.bgee.gene_expressions_in_anatomy",
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "ctd_exposure_protein_interactions": "silver.ctd.ctd_exposure_protein_interactions",
        "ctd_exposure_exposure_interactions": "silver.ctd.ctd_exposure_exposure_interactions",
        "drug_drug_interactions": "silver.drugbank.drug_drug",
        "drug_protein_interactions": "silver.drugbank.drug_protein",
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
