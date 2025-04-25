import polars as pl
from kedro.pipeline import node


def process_reactome(
    reactome_ncbi: pl.DataFrame,
    reactome_relations: pl.DataFrame,
    reactome_terms: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    reactome_ncbi = reactome_ncbi.sort(by=["ncbi_id", "reactome_id"])
    reactome_relations = reactome_relations.sort(by=["reactome_id_1", "reactome_id_2"])
    reactome_terms = reactome_terms.sort(by=["reactome_id"])
    return reactome_ncbi, reactome_relations, reactome_terms


reactome_node = node(
    process_reactome,
    inputs={
        "reactome_ncbi": "bronze.reactome.reactome_ncbi",
        "reactome_relations": "bronze.reactome.reactome_relations",
        "reactome_terms": "bronze.reactome.reactome_terms",
    },
    outputs=[
        "reactome.reactome_ncbi",
        "reactome.reactome_relations",
        "reactome.reactome_terms",
    ],
    name="reactome",
)
