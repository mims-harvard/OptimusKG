from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    return pipeline(
        [getattr(nodes, name) for name in nodes.__all__],
        namespace="silver",
        inputs={
            # Bronze
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
            "bronze.opentargets.disease_to_phenotype",
            "bronze.opentargets.targets",
            "bronze.opentargets.diseases",
            "bronze.opentargets.disease_phenotype_ids",
            "bronze.opentargets.drug_mappings",
            "bronze.ontology.go_terms",
            "bronze.ontology.go_relations",
            "bronze.ontology.mondo_terms",
            "bronze.ontology.mondo_relations",
            "bronze.ontology.mondo_xrefs",
            "bronze.ontology.phenotypes",
            "bronze.ontology.phenotypes_xrefs",
            "bronze.ontology.hpo_mappings",
            "bronze.bgee.gene_expressions_in_anatomy",
            "bronze.ctd.ctd_exposure_events",
            "bronze.drugcentral.drug_disease",
            "bronze.gene_names.protein_names",
            "bronze.ncbigene.protein_go_associations",
            "bronze.reactome.reactome_ncbi",
            "bronze.reactome.reactome_relations",
            "bronze.reactome.reactome_terms",
            "bronze.drugbank.drug_drug",
            "bronze.drugbank.drug_protein",
            "bronze.drugbank.vocabulary",
            "bronze.umls.mrconso",
            "bronze.disgenet.disgenet_diseases",
            "bronze.disgenet.disgenet_phenotypes",
            # Silver
            "silver.umls.umls_mondo",
        },
    )
