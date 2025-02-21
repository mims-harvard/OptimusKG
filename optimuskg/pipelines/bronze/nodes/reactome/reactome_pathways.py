import logging
from typing import Final, final

import polars as pl
from kedro.pipeline import node

log = logging.getLogger(__name__)


def process_reactome_pathways(
    reactome_pathways_relation: pl.DataFrame,
    reactome_pathways: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    df_terms = reactome_pathways.filter(pl.col("species") == "Homo sapiens")
    df_terms = df_terms.unique()

    # Use valid Reactome IDs for mapping relationships
    valid_terms = df_terms.select("reactome_id").to_series().to_list()

    df_relations = reactome_pathways_relation.filter(
        (pl.col("reactome_id_1").is_in(valid_terms))
        & (pl.col("reactome_id_2").is_in(valid_terms))
    )
    df_relations = df_relations.unique()

    return df_relations, df_terms


reactome_pathways_node = node(
    process_reactome_pathways,
    inputs={
        "reactome_pathways_relation": "landing.reactome.reactome_pathways_relation",
        "reactome_pathways": "landing.reactome.reactome_pathways",
    },
    outputs=[
        "reactome.reactome_relations",
        "reactome.reactome_terms",
    ],
    name="reactome_pathways",
)
