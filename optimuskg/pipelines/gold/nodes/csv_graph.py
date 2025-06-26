import logging

import polars as pl
from kedro.pipeline import node
from .edges.utils import normalize_edge_topology

logger = logging.getLogger(__name__)


def process_csv_graph(  # noqa: PLR0913
    # Edges
    anatomy_protein_absent: pl.DataFrame,
    anatomy_protein_present: pl.DataFrame,
    biological_process_protein: pl.DataFrame,
    cellular_component_protein: pl.DataFrame,
    disease_protein_negative: pl.DataFrame,
    disease_protein_positive: pl.DataFrame,
    disease_protein: pl.DataFrame,
    disease_disease: pl.DataFrame,
    drug_drug: pl.DataFrame,
    drug_protein: pl.DataFrame,
    exposure_exposure: pl.DataFrame,
    exposure_protein: pl.DataFrame,
    exposure_disease: pl.DataFrame,
    indication: pl.DataFrame,
    contraindication: pl.DataFrame,
    off_label_use: pl.DataFrame,
    molecular_function_protein: pl.DataFrame,
    pathway_pathway: pl.DataFrame,
    pathway_protein: pl.DataFrame,
    phenotype_protein: pl.DataFrame,
    strong_clinical_evidence: pl.DataFrame,
    weak_clinical_evidence: pl.DataFrame,
    exposure_biological_process: pl.DataFrame,
    exposure_molecular_function: pl.DataFrame,
    exposure_cellular_component: pl.DataFrame,
    # Nodes
    gene: pl.DataFrame,
    anatomical_entity: pl.DataFrame,
    environmental_exposure: pl.DataFrame,
    drug: pl.DataFrame,
    disease: pl.DataFrame,
    phenotype: pl.DataFrame,
    biological_process: pl.DataFrame,
    cellular_component: pl.DataFrame,
    molecular_function: pl.DataFrame,
    pathway: pl.DataFrame,
) -> None:
    # Concatenate all the edge DataFrames into a single DataFrame
    kg_edges = pl.concat(
        [
            anatomy_protein_absent,
            anatomy_protein_present,
            biological_process_protein,
            cellular_component_protein,
            disease_protein_negative,
            disease_protein_positive,
            disease_protein,
            disease_disease,
            drug_drug,
            drug_protein,
            exposure_exposure,
            exposure_protein,
            exposure_disease,
            indication,
            contraindication,
            off_label_use,
            molecular_function_protein,
            pathway_pathway,
            pathway_protein,
            phenotype_protein,
            strong_clinical_evidence,
            weak_clinical_evidence,
            exposure_biological_process,
            exposure_molecular_function,
            exposure_cellular_component,
        ]
    )
    kg_edges = normalize_edge_topology(kg_edges)

    # Concatenate all the node DataFrames into a single DataFrame
    kg_nodes = pl.concat(
        [
            gene,
            anatomical_entity,
            environmental_exposure,
            drug,
            disease,
            phenotype,
            biological_process,
            cellular_component,
            molecular_function,
            pathway,
        ]
    ).unique()

    return kg_edges, kg_nodes


csv_graph_node = node(
    process_csv_graph,
    inputs={
        # Edges
        "anatomy_protein_absent": "gold.edges.anatomy_protein_absent",
        "anatomy_protein_present": "gold.edges.anatomy_protein_present",
        "biological_process_protein": "gold.edges.biological_process_protein",
        "cellular_component_protein": "gold.edges.cellular_component_protein",
        "disease_protein_negative": "gold.edges.disease_protein_negative",
        "disease_protein_positive": "gold.edges.disease_protein_positive",
        "disease_protein": "gold.edges.disease_protein",
        "disease_disease": "gold.edges.disease_disease",
        "drug_drug": "gold.edges.drug_drug",
        "drug_protein": "gold.edges.drug_protein",
        "exposure_exposure": "gold.edges.exposure_exposure",
        "exposure_protein": "gold.edges.exposure_protein",
        "exposure_disease": "gold.edges.exposure_disease",
        "indication": "gold.edges.indication",
        "contraindication": "gold.edges.contraindication",
        "off_label_use": "gold.edges.off_label_use",
        "molecular_function_protein": "gold.edges.molecular_function_protein",
        "pathway_pathway": "gold.edges.pathway_pathway",
        "pathway_protein": "gold.edges.pathway_protein",
        "phenotype_protein": "gold.edges.phenotype_protein",
        "strong_clinical_evidence": "gold.edges.strong_clinical_evidence",
        "weak_clinical_evidence": "gold.edges.weak_clinical_evidence",
        "exposure_biological_process": "gold.edges.exposure_biological_process",
        "exposure_molecular_function": "gold.edges.exposure_molecular_function",
        "exposure_cellular_component": "gold.edges.exposure_cellular_component",
        # Nodes
        "gene": "gold.nodes.gene",
        "anatomical_entity": "gold.nodes.anatomical_entity",
        "environmental_exposure": "gold.nodes.environmental_exposure",
        "drug": "gold.nodes.drug",
        "disease": "gold.nodes.disease",
        "phenotype": "gold.nodes.phenotype",
        "biological_process": "gold.nodes.biological_process",
        "cellular_component": "gold.nodes.cellular_component",
        "molecular_function": "gold.nodes.molecular_function",
        "pathway": "gold.nodes.pathway",
    },
    outputs=["optimuskg_edges", "optimuskg_nodes"],
    tags=["gold"],
    name="csv_graph",
)
