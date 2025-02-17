import polars as pl
from kedro.pipeline import node


def process_ncbigene(
    ncbigene_protein_go_associations: pl.DataFrame,
) -> pl.DataFrame:
    return ncbigene_protein_go_associations


ncbigene_node = node(
    process_ncbigene,
    inputs={
        "ncbigene_protein_go_associations": "bronze.ncbigene.protein_go_associations"
    },
    outputs="ncbigene.protein_go_associations",
    name="ncbigene",
)
