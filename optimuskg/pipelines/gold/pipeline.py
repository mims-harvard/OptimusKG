from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    return pipeline(
        [getattr(nodes, name) for name in nodes.__all__],
        namespace="gold",
        inputs={
            # Silver
            "silver.bgee.gene_expressions_in_anatomy",
            "silver.opentargets.opentargets_edges",
            "silver.ctd.ctd_exposure_protein_interactions",
            "silver.ctd.ctd_exposure_exposure_interactions",
            "silver.ctd.ctd_exposure_disease_interactions",
            "silver.drugbank.drug_drug",
            "silver.drugbank.drug_protein",
            "silver.ncbigene.protein_biological_process_interactions",
            "silver.ncbigene.protein_cellular_component_interactions",
            "silver.ncbigene.protein_molecular_function_interactions",
            "silver.reactome.pathway_pathway_interactions",
            "silver.reactome.pathway_protein_interactions",
            "silver.ontology.mondo_disease_disease_interactions",
            # Nodes
            "gold.nodes.gene",
            "gold.nodes.anatomical_entity",
            "gold.nodes.environmental_exposure",
            "gold.nodes.drug",
            "gold.nodes.disease",
            "gold.nodes.phenotype",
            "gold.nodes.biological_process",
            "gold.nodes.cellular_component",
            "gold.nodes.molecular_function",
            "gold.nodes.pathway",
            # Edges
            "gold.edges.anatomy_protein_absent",
            "gold.edges.anatomy_protein_present",
            "gold.edges.biological_process_protein",
            "gold.edges.cellular_component_protein",
            "gold.edges.disease_protein_negative",
            "gold.edges.disease_protein_positive",
            "gold.edges.disease_protein",
            "gold.edges.disease_disease",
            "gold.edges.drug_drug",
            "gold.edges.drug_protein",
            "gold.edges.exposure_exposure",
            "gold.edges.exposure_protein",
            "gold.edges.exposure_disease",
            "gold.edges.indication",
            "gold.edges.molecular_function_protein",
            "gold.edges.pathway_pathway",
            "gold.edges.pathway_protein",
            "gold.edges.phenotype_protein",
            "gold.edges.strong_clinical_evidence",
            "gold.edges.weak_clinical_evidence",
        },
    )
