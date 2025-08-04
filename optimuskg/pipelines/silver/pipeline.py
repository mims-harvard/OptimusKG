from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    return pipeline(
        [getattr(nodes, name) for name in nodes.__all__],
        namespace="silver",
        inputs={
            # Bronze
            "bronze.ontology.go_terms",
            "bronze.ontology.go_relations",
            "bronze.ontology.mondo_terms",
            "bronze.ontology.mondo_xrefs",
            "bronze.ontology.hp_xrefs",
            "bronze.ontology.hp_relations",
            "bronze.ontology.uberon_terms",
            "bronze.ontology.uberon_relations",
            "bronze.bgee.gene_expressions_in_anatomy",
            "bronze.ctd.ctd_exposure_events",
            "bronze.drug_disease",
            "bronze.gene_names.protein_names",
            "bronze.reactome.reactome_relations",
            "bronze.drug_drug",
            "bronze.drug_protein",
            "bronze.drugbank.vocabulary",
            "bronze.umls.mrconso",
            "bronze.disgenet.diseases",
            "bronze.disgenet.disgenet_phenotypes",
            "bronze.onsides.high_confidence",
            "bronze.opentargets.target_disease_associations",
            "bronze.opentargets.drug_indication",
            "bronze.opentargets.disease",
            "bronze.opentargets.target",
            "bronze.opentargets.disease_phenotype",
            "bronze.opentargets.drug_mechanism_of_action",
            "bronze.opentargets.drug_molecule",
            # Silver
            "silver.umls.umls_mondo",
        },
    )
