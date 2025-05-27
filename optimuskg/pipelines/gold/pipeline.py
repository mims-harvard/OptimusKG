from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    # return None  # TODO: This is for ignoring the golden pipeline in the execution
    return pipeline(
        [getattr(nodes, name) for name in nodes.__all__],
        namespace="gold",
        inputs={
            # Data
            "silver.bgee.gene_expressions_in_anatomy",
            "silver.opentargets.opentargets_edges",
            "silver.ctd.ctd_exposure_protein_interactions",
            "silver.ctd.ctd_exposure_exposure_interactions",
            "silver.drugbank.drug_drug",
            "silver.drugbank.drug_protein",
            # "silver.ncbigene.protein_biological_process_interactions",
            # "silver.ncbigene.protein_cellular_component_interactions",
            # "silver.ncbigene.protein_molecular_function_interactions",
            # "silver.reactome.pathway_pathway_interactions",
            # "silver.reactome.pathway_protein_interactions",
        },
    )
