import polars as pl
from kedro.pipeline import node


def run(
    high_confidence: pl.DataFrame,
) -> pl.DataFrame:
    return (
        high_confidence.rename(
            {
                "ingredient_id": "x_id",
                "effect_meddra_id": "y_id",
                "rxnorm_name": "x_name",
                "meddra_name": "y_name",
            }
        )
        .with_columns(
            [
                pl.lit("drug").alias("x_type"),
                pl.lit("OnSIDES").alias("x_source"),
                pl.lit("phenotype").alias("y_type"),
                pl.lit("OnSIDES").alias("y_source"),
                pl.lit("drug_phenotype").alias("relation"),
                pl.lit("adverse_drug_reaction").alias("relation_type"),
            ]
        )
        .select(
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
    )


drug_phenotype_node = node(
    run,
    inputs={
        "high_confidence": "bronze.onsides.high_confidence",
    },
    outputs="drug_phenotype",
    name="onsides",
    tags=["silver"],
)
