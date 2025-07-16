import polars as pl
from kedro.pipeline import node


def run(
    hpo_terms: pl.DataFrame,
    hpo_relations: pl.DataFrame,
) -> pl.DataFrame:
    df_phenotype_phenotype = hpo_relations.join(
        hpo_terms, left_on="parent", right_on="id", how="left"
    )

    df_phenotype_phenotype = df_phenotype_phenotype.rename({"name": "parent_name"})

    df_phenotype_phenotype = df_phenotype_phenotype.join(
        hpo_terms, left_on="child", right_on="id", how="left"
    )

    df_phenotype_phenotype = df_phenotype_phenotype.rename({"name": "child_name"})

    df_phenotype_phenotype = df_phenotype_phenotype.rename(
        {
            "parent": "x_id",
            "child": "y_id",
            "parent_name": "x_name",
            "child_name": "y_name",
        }
    )

    df_phenotype_phenotype = df_phenotype_phenotype.with_columns(
        [
            pl.lit("phenotype").alias("x_type"),
            pl.lit("HPO").alias("x_source"),
            pl.lit("phenotype").alias("y_type"),
            pl.lit("HPO").alias("y_source"),
            pl.lit("phenotype_phenotype").alias("relation"),
            pl.lit("parent-child").alias("relation_type"),
        ]
    )

    df_phenotype_phenotype = df_phenotype_phenotype.select(
        [
            "relation",
            "relation_type",
            "x_id",
            "x_type",
            "x_name",
            "x_source",
            "y_id",
            "y_type",
            "y_name",
            "y_source",
        ]
    )

    return df_phenotype_phenotype


phenotype_phenotype_node = node(
    run,
    inputs={
        "hpo_terms": "bronze.ontology.hpo_terms",
        "hpo_relations": "bronze.ontology.hpo_relations",
    },
    outputs="ontology.phenotype_phenotype",
    name="phenotype_phenotype",
    tags=["silver"],
)
