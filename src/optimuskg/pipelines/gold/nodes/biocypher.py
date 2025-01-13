import logging

import polars as pl
from biocypher import BioCypher, _ontology
from kedro.pipeline import node
from typedframe import PolarsTypedFrame, TypedDataFrame

log = logging.getLogger(__name__)


def process_biocypher(
    homo_sapiens_expressions_advanced: pl.DataFrame,
    biolink_ontology: "landing.biolink.ontology",
    disease_ontology: "landing.disease_ontology.ontology",
    gene_ontology: "landing.gene_ontology.ontology",
    human_phenotype_ontology: "landing.human_phenotype_ontology.ontology",
    mondo_ontology: "landing.mondo_ontology",
    orphanet_ontology: "landing.orphanet.ontology",
    uberon_ontology: "landing.uber_anatomy_ontology.ontology",
) -> None:
    # bc = BioCypher(head_ontology={
    #     url=""
    # }, tail_ontologies={})
    # bc._ontology = _ontology.Ontology()
    return None


biocypher_node = node(
    process_biocypher,
    {
        "biolink_ontology": "landing.biolink.ontology",
        "disease_ontology": "landing.disease_ontology.ontology",
        "gene_ontology": "landing.gene_ontology.ontology",
        "human_phenotype_ontology": "landing.human_phenotype_ontology.ontology",
        "mondo_ontology": "landing.mondo_ontology",
        "orphanet_ontology": "landing.orphanet.ontology",
        "uberon_ontology": "landing.uber_anatomy_ontology.ontology",
    },
    "biocypher_graph",
    name="bgee",
)
