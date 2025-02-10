import logging

import polars as pl
from biocypher import BioCypher
from kedro.pipeline import node

from optimuskg.datasets.owl_dataset import LoadedOWLDataset  # type: ignore

log = logging.getLogger(__name__)


def process_biocypher(  # noqa: PLR0913
    biolink_ontology: LoadedOWLDataset,
    disease_ontology: LoadedOWLDataset,
    gene_ontology: LoadedOWLDataset,
    human_phenotype_ontology: LoadedOWLDataset,
    mondo_ontology: LoadedOWLDataset,
    orphanet_ontology: LoadedOWLDataset,
    uberon_ontology: LoadedOWLDataset,
) -> pl.DataFrame:
    bc = BioCypher(
        head_ontology={
            "url": biolink_ontology.filepath,
            "root_node": "entity",
            "format": "ttl",
        },
        tail_ontologies={
            # "doid": {
            #     "url": disease_ontology.filepath,
            #     "head_join_node": "disease",
            #     "tail_join_node": "disease",
            #     "merge_nodes": False,
            # },
            # "go": {
            #     "url": gene_ontology.filepath,
            #     "head_join_node": "disease",
            #     "tail_join_node": "human disease",
            #     "merge_nodes": False,
            # },
            # "hpo": {
            #     "url": human_phenotype_ontology.filepath,
            #     "head_join_node": "disease",
            #     "tail_join_node": "human disease",
            #     "merge_nodes": False,
            # },
            "mondo": {
                "url": mondo_ontology.filepath,
                "head_join_node": "disease",
                "tail_join_node": "human disease",
                "merge_nodes": False,
            },
            # "ordo": {
            #     "url": orphanet_ontology.filepath,
            #     "head_join_node": "disease",
            #     "tail_join_node": "human disease",
            #     "merge_nodes": False,
            # },
            # "uberon": {
            #     "url": uberon_ontology.filepath,
            #     "head_join_node": "disease",
            #     "tail_join_node": "human disease",
            #     "merge_nodes": False,
            #     "format": "owl",
            # },
        },
    )
    try:
        bc.show_ontology_structure(
            full=True,
            to_disk="data/gold/neo4j_import_volume",
        )
    except Exception as e:
        # TODO: Remove this once we have a way to handle this error
        log.error(f"Error showing ontology structure... skipping")
        raise
    return pl.DataFrame()


biocypher_node = node(
    process_biocypher,
    inputs={
        "biolink_ontology": "landing.biolink.ontology",
        "disease_ontology": "landing.disease_ontology.ontology",
        "gene_ontology": "landing.gene_ontology.ontology",
        "human_phenotype_ontology": "landing.human_phenotype_ontology.ontology",
        "mondo_ontology": "landing.mondo.ontology",
        "orphanet_ontology": "landing.orphanet.ontology",
        "uberon_ontology": "landing.uber_anatomy_ontology.ontology",
    },
    outputs="biocypher_graph",
    name="biocypher",
)
