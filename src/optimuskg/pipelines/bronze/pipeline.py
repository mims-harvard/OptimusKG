from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    return pipeline(
        [getattr(nodes, name) for name in nodes.__all__],
        namespace="bronze",
        inputs={
            "landing.bgee.homo_sapiens_expressions_advanced",
            "landing.ctd.ctd_exposure_events",
            "landing.reactome.ncbi2_reactome",
            "landing.reactome.reactome_pathways_relation",
            "landing.reactome.reactome_pathways",
            "landing.gene_names.gene_names",
            "landing.ncbigene.gene2go",
        },
    )
