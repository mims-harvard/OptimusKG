from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    return pipeline(
        [getattr(nodes, name) for name in nodes.__all__],
        namespace="bronze",
        inputs={
            # landing
            "landing.opentargets.target_disease_associations",
            "landing.opentargets.target",
            "landing.opentargets.drug_molecule",
            "landing.opentargets.disease",
            "landing.opentargets.disease_phenotype",
            "landing.opentargets.drug_indication",
            "landing.opentargets.drug_mechanism_of_action",
            "landing.ontology.go_plus",
            "landing.ontology.mondo",
            "landing.ontology.hp",
            "landing.ontology.hp_mappings",
            "landing.ontology.uberon",
            "landing.bgee.homo_sapiens_expressions_advanced",
            "landing.ctd.ctd_exposure_events",
            "landing.reactome.ncbi2_reactome",
            "landing.reactome.reactome_pathways_relation",
            "landing.reactome.reactome_pathways",
            "landing.gene_names.gene_names",
            "landing.ncbigene.gene2go",
            "landing.drugbank.full_database",
            "landing.drugbank.carrier",
            "landing.drugbank.enzyme",
            "landing.drugbank.target",
            "landing.drugbank.transporter",
            "landing.drugbank.gene_map",
            "landing.drugbank.vocabulary",
            "landing.drugcentral.psql_dump",
            "landing.umls.mrconso",
            "landing.disgenet.curated_gene_disease_associations",
            "landing.onsides.high_confidence",
            "landing.onsides.vocab_rxnorm_ingredient",
            "landing.onsides.vocab_meddra_adverse_effect",
            # bronze
            "bronze.drugbank.vocabulary",
        },
    )
