from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    # return None  # TODO: This is for ignoring the golden pipeline in the execution
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
