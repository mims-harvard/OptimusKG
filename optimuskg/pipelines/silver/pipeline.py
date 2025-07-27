from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    return pipeline(
        [getattr(nodes, name) for name in nodes.__all__],
        namespace="silver",
        inputs={
            # Bronze
            "bronze.opentargets.evidence",
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
            "bronze.ontology.hp_terms",
            "bronze.ontology.hp_xrefs",
            "bronze.ontology.hp_relations",
            "bronze.ontology.hp_mappings",
            "bronze.ontology.uberon_terms",
            "bronze.ontology.uberon_relations",
            "bronze.bgee.gene_expressions_in_anatomy",
            "bronze.ctd.ctd_exposure_events",
            "bronze.drug_disease",
            "bronze.gene_names.protein_names",
            "bronze.ncbigene.protein_go_associations",
            "bronze.reactome.reactome_ncbi",
            "bronze.reactome.reactome_relations",
            "bronze.reactome.reactome_terms",
            "bronze.drug_drug",
            "bronze.drug_protein",
            "bronze.drugbank.vocabulary",
            "bronze.umls.mrconso",
            "bronze.disgenet.diseases",
            "bronze.disgenet.disgenet_phenotypes",
            "bronze.onsides.high_confidence",
            # Silver
            "silver.umls.umls_mondo",
        },
    )
