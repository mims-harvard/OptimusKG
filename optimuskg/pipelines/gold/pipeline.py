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
        inputs={
            "landing.ontology.biolink",
            "landing.ontology.disease",
            "landing.ontology.gene",
            "landing.ontology.human_phenotype",
            "landing.ontology.mondo",
            "landing.ontology.orphanet",
            "landing.ontology.uber_anatomy",
        },
    )
