from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    return pipeline(
        [getattr(nodes, name) for name in nodes.__all__],
        namespace="gold",
        inputs={
            # Nodes
            "silver.nodes.gene",
            "silver.nodes.anatomy",
            "silver.nodes.exposure",
            "silver.nodes.drug",
            "silver.nodes.disease",
            "silver.nodes.phenotype",
            "silver.nodes.biological_process",
            "silver.nodes.cellular_component",
            "silver.nodes.molecular_function",
            "silver.nodes.pathway",
            # Edges
            "silver.edges.anatomy_protein",
            "silver.edges.anatomy_anatomy",
            "silver.edges.biological_process_protein",
            "silver.edges.exposure_biological_process",
            "silver.edges.biological_process_biological_process",
            "silver.edges.cellular_component_protein",
            "silver.edges.exposure_cellular_component",
            "silver.edges.cellular_component_cellular_component",
            "silver.edges.disease_disease",
            "silver.edges.exposure_disease",
            "silver.edges.drug_disease",
            "silver.edges.disease_protein",
            "silver.edges.disease_phenotype",
            "silver.edges.drug_drug",
            "silver.edges.drug_protein",
            "silver.edges.drug_phenotype",
            "silver.edges.exposure_protein",
            "silver.edges.exposure_exposure",
            "silver.edges.exposure_molecular_function",
            "silver.edges.molecular_function_protein",
            "silver.edges.pathway_protein",
            "silver.edges.phenotype_protein",
            "silver.edges.molecular_function_molecular_function",
            "silver.edges.pathway_pathway",
            "silver.edges.phenotype_phenotype",
        },
    )
