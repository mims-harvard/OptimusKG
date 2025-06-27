import polars as pl
from kedro.pipeline import node


def process_effect_effect(
    phenotypes: pl.DataFrame,
    phenotypes_parents: pl.DataFrame,
) -> pl.DataFrame:
    df_effect_effect = phenotypes_parents.join(
        phenotypes, left_on="parent", right_on="id", how="left"
    )

    df_effect_effect = df_effect_effect.rename({"name": "parent_name"})

    df_effect_effect = df_effect_effect.join(
        phenotypes, left_on="child", right_on="id", how="left"
    )

    df_effect_effect = df_effect_effect.rename({"name": "child_name"})

    df_effect_effect = df_effect_effect.rename(
        {
            "parent": "x_id",
            "child": "y_id",
            "parent_name": "x_name",
            "child_name": "y_name",
        }
    )

    df_effect_effect = df_effect_effect.with_columns(
        [
            pl.lit("phenotype").alias("x_type"),
            pl.lit("HPO").alias("x_source"),
            pl.lit("phenotype").alias("y_type"),
            pl.lit("HPO").alias("y_source"),
            pl.lit("phenotype_phenotype").alias("relation"),
            pl.lit("parent-child").alias("relation_type"),
        ]
    )

    df_effect_effect = df_effect_effect.select(
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

    return df_effect_effect


effect_effect_node = node(
    process_effect_effect,
    inputs={
        "phenotypes": "bronze.opentargets.phenotypes",
        "phenotypes_parents": "bronze.opentargets.phenotypes_parents",
    },
    outputs="effect_effect",
    name="effect_effect",
    tags=["silver"],
)
