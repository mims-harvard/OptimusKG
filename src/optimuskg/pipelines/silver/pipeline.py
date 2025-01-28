from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    return pipeline(
        [getattr(nodes, name) for name in nodes.__all__],
        namespace="silver",
        inputs={
            "bronze.opentargets.evidence.cancer_gene_census",
            "bronze.opentargets.evidence.chembl",
            "bronze.opentargets.evidence.clingen",
            "bronze.opentargets.evidence.crispr",
            "bronze.opentargets.evidence.crispr_screen",
            "bronze.opentargets.evidence.expression_atlas",
            "bronze.opentargets.evidence.gene_burden",
            "bronze.opentargets.evidence.gene2phenotype",
            "bronze.opentargets.evidence.genomics_england",
            "bronze.opentargets.evidence.intogen",
            "bronze.opentargets.evidence.progeny",
            "bronze.opentargets.evidence.reactome",
            "bronze.opentargets.evidence.slapenrich",
            "bronze.opentargets.evidence.sysbio",
            "bronze.opentargets.evidence.uniprot_literature",
            "bronze.opentargets.evidence.orphanet",
            "landing.opentargets.primekg_nodes",
            "landing.opentargets.primekg_edges",
            "bronze.opentargets.disease_to_phenotype",
            "bronze.opentargets.targets",
            "bronze.opentargets.diseases",
            "bronze.opentargets.phenotypes",
            "bronze.opentargets.disease_phenotype_ids",
            "bronze.opentargets.drug_mappings",
        },
    )
