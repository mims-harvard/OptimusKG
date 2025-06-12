import polars as pl
from kedro.pipeline import node


def process_disease_disease_interactions(
    mondo_terms: pl.DataFrame,
    mondo_relations: pl.DataFrame,
) -> pl.DataFrame:
    # Filter for is_a relationships
    mondo_parents = mondo_relations.filter(pl.col("edge_type") == "is_a")

    # Join with mondo_terms to get parent names ('head')
    df = mondo_parents.join(
        mondo_terms.select("id", "name"), left_on="head", right_on="id", how="left"
    )

    # Join with mondo_terms to get child names ('tail')
    df = df.join(
        mondo_terms.select("id", "name"),
        left_on="tail",
        right_on="id",
        how="left",
        suffix="_child",
    )

    df = df.rename(
        {
            "head": "x_id",
            "name": "x_name",
            "tail": "y_id",
            "name_child": "y_name",
        }
    )

    df_disease_disease = df.with_columns(
        pl.lit("disease").alias("x_type"),
        pl.lit("MONDO").alias("x_source"),
        pl.lit("disease").alias("y_type"),
        pl.lit("MONDO").alias("y_source"),
        pl.lit("disease_disease").alias("relation"),
        pl.lit("parent-child").alias("relation_type"),
    )

    final_cols = [
        "relation",
        "relation_type",
        "x_id",
        "x_name",
        "x_type",
        "x_source",
        "y_id",
        "y_name",
        "y_type",
        "y_source",
    ]

    df_disease_disease = df_disease_disease.select(final_cols)

    return df_disease_disease


disease_disease_interactions_node = node(
    process_disease_disease_interactions,
    inputs={
        "mondo_terms": "bronze.ontology.mondo_terms",
        "mondo_relations": "bronze.ontology.mondo_relations",
    },
    outputs="ontology.mondo_disease_disease_interactions",
    name="disease_disease_interactions",
    tags=["silver"],
)
