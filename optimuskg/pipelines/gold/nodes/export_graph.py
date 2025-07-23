import logging

import polars as pl
from kedro.pipeline import node

from .export import export_to_biocypher, export_to_csv

logger = logging.getLogger(__name__)


def run(  # noqa: PLR0913
    # Nodes
    gene: pl.DataFrame,
    anatomy: pl.DataFrame,
    exposure: pl.DataFrame,
    drug: pl.DataFrame,
    disease: pl.DataFrame,
    phenotype: pl.DataFrame,
    biological_process: pl.DataFrame,
    cellular_component: pl.DataFrame,
    molecular_function: pl.DataFrame,
    pathway: pl.DataFrame,
    # Edges
    anatomy_protein: pl.DataFrame,
    biological_process_protein: pl.DataFrame,
    cellular_component_protein: pl.DataFrame,
    disease_protein: pl.DataFrame,
    disease_disease: pl.DataFrame,
    disease_phenotype: pl.DataFrame,
    drug_drug: pl.DataFrame,
    drug_protein: pl.DataFrame,
    drug_disease: pl.DataFrame,
    exposure_exposure: pl.DataFrame,
    exposure_protein: pl.DataFrame,
    exposure_disease: pl.DataFrame,
    molecular_function_protein: pl.DataFrame,
    pathway_pathway: pl.DataFrame,
    pathway_protein: pl.DataFrame,
    phenotype_protein: pl.DataFrame,
    exposure_biological_process: pl.DataFrame,
    exposure_molecular_function: pl.DataFrame,
    exposure_cellular_component: pl.DataFrame,
    cellular_component_cellular_component: pl.DataFrame,
    biological_process_biological_process: pl.DataFrame,
    molecular_function_molecular_function: pl.DataFrame,
    phenotype_phenotype: pl.DataFrame,
    anatomy_anatomy: pl.DataFrame,
    drug_phenotype: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    nodes = {
        "gene": gene,
        "anatomy": anatomy,
        "exposure": exposure,
        "drug": drug,
        "disease": disease,
        "phenotype": phenotype,
        "biological process": biological_process,
        "cellular component": cellular_component,
        "molecular function": molecular_function,
        "pathway": pathway,
    }
    edges = {
        "anatomy protein": anatomy_protein,
        "biological process protein": biological_process_protein,
        "cellular component protein": cellular_component_protein,
        "disease protein": disease_protein,
        "disease disease": disease_disease,
        "disease phenotype": disease_phenotype,
        "drug drug": drug_drug,
        "drug protein": drug_protein,
        "drug disease": drug_disease,
        "exposure exposure": exposure_exposure,
        "exposure protein": exposure_protein,
        "exposure disease": exposure_disease,
        "molecular function protein": molecular_function_protein,
        "pathway pathway": pathway_pathway,
        "pathway protein": pathway_protein,
        "phenotype protein": phenotype_protein,
        "exposure biological process": exposure_biological_process,
        "exposure molecular function": exposure_molecular_function,
        "exposure cellular component": exposure_cellular_component,
        "cellular component cellular component": cellular_component_cellular_component,
        "biological process biological process": biological_process_biological_process,
        "molecular function molecular function": molecular_function_molecular_function,
        "phenotype phenotype": phenotype_phenotype,
        "anatomy anatomy": anatomy_anatomy,
        "drug phenotype": drug_phenotype,
    }

    export_to_biocypher(edges, nodes)
    kg_edges, kg_nodes = export_to_csv(list(edges.values()), list(nodes.values()))

    return kg_edges, kg_nodes


export_graph_node = node(
    run,
    inputs={
        # Nodes
        "gene": "gold.nodes.gene",
        "anatomy": "gold.nodes.anatomy",
        "exposure": "gold.nodes.exposure",
        "drug": "gold.nodes.drug",
        "disease": "gold.nodes.disease",
        "phenotype": "gold.nodes.phenotype",
        "biological_process": "gold.nodes.biological_process",
        "cellular_component": "gold.nodes.cellular_component",
        "molecular_function": "gold.nodes.molecular_function",
        "pathway": "gold.nodes.pathway",
        # Edges
        "anatomy_protein": "gold.edges.anatomy_protein",
        "biological_process_protein": "gold.edges.biological_process_protein",
        "cellular_component_protein": "gold.edges.cellular_component_protein",
        "disease_protein": "gold.edges.disease_protein",
        "disease_disease": "gold.edges.disease_disease",
        "disease_phenotype": "gold.edges.disease_phenotype",
        "drug_drug": "gold.edges.drug_drug",
        "drug_protein": "gold.edges.drug_protein",
        "drug_disease": "gold.edges.drug_disease",
        "exposure_exposure": "gold.edges.exposure_exposure",
        "exposure_protein": "gold.edges.exposure_protein",
        "exposure_disease": "gold.edges.exposure_disease",
        "molecular_function_protein": "gold.edges.molecular_function_protein",
        "pathway_pathway": "gold.edges.pathway_pathway",
        "pathway_protein": "gold.edges.pathway_protein",
        "phenotype_protein": "gold.edges.phenotype_protein",
        "exposure_biological_process": "gold.edges.exposure_biological_process",
        "exposure_molecular_function": "gold.edges.exposure_molecular_function",
        "exposure_cellular_component": "gold.edges.exposure_cellular_component",
        "cellular_component_cellular_component": "gold.edges.cellular_component_cellular_component",
        "biological_process_biological_process": "gold.edges.biological_process_biological_process",
        "molecular_function_molecular_function": "gold.edges.molecular_function_molecular_function",
        "phenotype_phenotype": "gold.edges.phenotype_phenotype",
        "anatomy_anatomy": "gold.edges.anatomy_anatomy",
        "drug_phenotype": "gold.edges.drug_phenotype",
    },
    outputs=["optimuskg_edges", "optimuskg_nodes"],
    tags=["gold"],
    name="export_graph",
)
