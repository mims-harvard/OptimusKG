from kedro.pipeline import pipeline

from . import nodes


def create_pipeline(**kwargs):
    return pipeline(
        [getattr(nodes, name) for name in nodes.__all__],
        namespace="bronze",
        inputs={
            # landing
            "landing.ontology.go_plus",
            "landing.ontology.human_phenotype",
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
            "landing.opentargets.evidence.cancer_gene_census",
            "landing.opentargets.evidence.chembl",
            "landing.opentargets.evidence.clingen",
            "landing.opentargets.evidence.crispr",
            "landing.opentargets.evidence.crispr_screen",
            "landing.opentargets.evidence.expression_atlas",
            "landing.opentargets.evidence.gene_burden",
            "landing.opentargets.evidence.gene2phenotype",
            "landing.opentargets.evidence.genomics_england",
            "landing.opentargets.evidence.intogen",
            "landing.opentargets.evidence.progeny",
            "landing.opentargets.evidence.reactome",
            "landing.opentargets.evidence.slapenrich",
            "landing.opentargets.evidence.sysbio",
            "landing.opentargets.evidence.uniprot_literature",
            "landing.opentargets.evidence.orphanet",
            "landing.opentargets.disease_to_phenotype",
            "landing.opentargets.targets",
            "landing.opentargets.primekg_nodes",
            "landing.opentargets.drug_mappings",
            "landing.opentargets.molecule",
            "landing.opentargets.mondo_efo_mappings",
            "landing.opentargets.diseases",
            # bronze
            "bronze.drugbank.vocabulary",
        },
    )
