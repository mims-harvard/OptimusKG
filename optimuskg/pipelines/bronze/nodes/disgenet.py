import polars as pl
from kedro.pipeline import node


def process_disgenet(
    curated_gene_disease_associations: pl.DataFrame,
) -> pl.DataFrame:
    
    disgenet_phenotypes = curated_gene_disease_associations.filter(
        pl.col("diseaseType") == "phenotype"
    )
    disgenet_diseases = curated_gene_disease_associations.filter(pl.col("diseaseType") == "disease")
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
