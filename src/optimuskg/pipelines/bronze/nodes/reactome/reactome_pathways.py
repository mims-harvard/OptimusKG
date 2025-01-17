import logging
from typing import Final, final

import polars as pl
from kedro.pipeline import node
from typedframe import PolarsTypedFrame

log = logging.getLogger(__name__)


@final
class ReactomeRelations(PolarsTypedFrame):
    schema: Final[dict[str, type[pl.DataType]]] = {
        "reactome_id_1": pl.String,
        "reactome_id_2": pl.String,
    }


@final
class ReactomeTerms(PolarsTypedFrame):
    schema: Final[dict[str, type[pl.DataType]]] = {
        "reactome_id": pl.String,
        "reactome_name": pl.String,
        "species": pl.String,
    }


def process_reactome_pathways(
    reactome_pathways_relation: pl.DataFrame,
    reactome_pathways: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    df_terms = ReactomeTerms.convert(reactome_pathways).df
    df_terms = df_terms.filter(pl.col("species") == "Homo sapiens")
    df_terms = df_terms.unique()

    reactome_terms = ReactomeTerms.convert(df_terms).df

    # Use valid Reactome IDs for mapping relationships
    valid_terms = reactome_terms.select("reactome_id").to_series().to_list()

    df_rels = reactome_pathways_relation.filter(
        (pl.col("reactome_id_1").is_in(valid_terms))
        & (pl.col("reactome_id_2").is_in(valid_terms))
    )
    df_rels = df_rels.unique()

    reactome_relations = ReactomeRelations.convert(df_rels).df

    return reactome_relations, reactome_terms


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
