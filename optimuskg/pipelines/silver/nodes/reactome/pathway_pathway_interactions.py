import logging

import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.utils import clean_edges

logger = logging.getLogger(__name__)


def process_pathway_pathway_interactions(
    reactome_relations: pl.DataFrame,
    reactome_terms: pl.DataFrame,
) -> pl.DataFrame:
    # Join reactome relations with terms for the first pathway
    df_path_path = reactome_relations.join(
        reactome_terms, left_on="reactome_id_1", right_on="reactome_id", how="inner"
    )

    # Rename columns for the first pathway
    df_path_path = df_path_path.with_columns(
        [pl.col("reactome_id_1").alias("x_id"), pl.col("reactome_name").alias("x_name")]
    )

    # Join with terms for the second pathway
    df_path_path = df_path_path.join(
        reactome_terms, left_on="reactome_id_2", right_on="reactome_id", how="inner"
    )

    # Rename columns for the second pathway
    df_path_path = df_path_path.with_columns(
        [pl.col("reactome_id_2").alias("y_id"), pl.col("reactome_name").alias("y_name")]
    )

    # Add constant columns
    df_path_path = df_path_path.with_columns(
        [
            pl.lit("REACTOME").alias("x_source"),
            pl.lit("pathway").alias("x_type"),
            pl.lit("REACTOME").alias("y_source"),
            pl.lit("pathway").alias("y_type"),
            pl.lit("pathway_pathway").alias("relation"),
            pl.lit("parent-child").alias("display_relation"),
        ]
    )

    # Clean edges
    df_path_path = clean_edges(df_path_path)

    return df_path_path


pathway_pathway_interactions_node = node(
    process_pathway_pathway_interactions,
    inputs={
        "reactome_relations": "bronze.reactome.reactome_relations",
        "reactome_terms": "bronze.reactome.reactome_terms",
    },
    outputs="reactome.pathway_pathway_interactions",
    name="pathway_pathway_interactions",
    tags=["silver"],
)
