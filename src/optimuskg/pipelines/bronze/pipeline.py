from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    return pipeline(
        [getattr(nodes, name) for name in nodes.__all__],
        namespace="bronze",
        inputs={"landing.bgee.homo_sapiens_expressions_advanced"},
    )
