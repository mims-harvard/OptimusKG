"""Example code for the nodes in the example pipeline. This code is meant
just for illustrating basic Kedro features.

Delete this when you start working on your own Kedro project.
"""

from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    return pipeline(
        [getattr(nodes, name) for name in nodes.__all__],
        namespace="gold",
        inputs=[
            "landing.biolink.ontology",
            "landing.disease_ontology.ontology",
            "landing.gene_ontology.ontology",
            "landing.human_phenotype_ontology.ontology",
            "landing.mondo.ontology",
            "landing.orphanet.ontology",
            "landing.uber_anatomy_ontology.ontology",
        ],
    )
