import polars as pl
from kedro.pipeline import node


def process_disgenet(
    curated_gene_disease_associations: pl.DataFrame,
) -> pl.DataFrame:
    
    curated_gene_disease_associations = curated_gene_disease_associations.rename({
        "geneid": "gene_id",
        "geneSymbol": "gene_symbol",
        "DSI": "dsi",
        "DPI": "dpi",
        "diseaseId": "disease_id",
        "diseaseName": "disease_name",
        "diseaseType": "disease_type",
        "diesaseClass": "disease_class",  # NOTE: fixing typo from "diesase" to "disease"
        "diesaseSemanticType": "disease_semantic_type",  # NOTE: fixing typo from "diesase" to "disease"
        "score": "score",
        "EI": "ei",
        "YearInitial": "year_initial",
        "YearFinal": "year_final",
        "NofPmids": "nof_pmids",
        "NofSnps": "nof_snps",
        "source": "source"
    })
    
    disgenet_phenotypes = curated_gene_disease_associations.filter(
        pl.col("disease_type") == "phenotype"
    )
    disgenet_diseases = curated_gene_disease_associations.filter(pl.col("disease_type") == "disease")
    return disgenet_phenotypes, disgenet_diseases


disgenet_node = node(
    process_disgenet,
    inputs={
        "curated_gene_disease_associations": "landing.disgenet.curated_gene_disease_associations",
    },
    outputs=["disgenet.disgenet_phenotypes", "disgenet.disgenet_diseases"],
    name="disgenet",
    tags=["bronze"],
)
