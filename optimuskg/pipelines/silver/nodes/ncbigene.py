import polars as pl
from kedro.pipeline import node


def process_ncbigene(
    ncbigene_protein_go_associations: pl.DataFrame,
) -> pl.DataFrame:
    df = ncbigene_protein_go_associations.sort(by=["ncbi_gene_id", "go_term_id"])
    return df


ncbigene_node = node(
    process_ncbigene,
    inputs={
        "ncbigene_protein_go_associations": "bronze.ncbigene.protein_go_associations"
    },
    outputs="ncbigene.protein_go_associations",
    name="ncbigene",
    tags=["silver"],
)
