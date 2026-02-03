import polars as pl
from kedro.pipeline import node


def run(
    high_confidence: pl.DataFrame,
    vocab_rxnorm_ingredient: pl.DataFrame,
    vocab_meddra_adverse_effect: pl.DataFrame,
) -> pl.DataFrame:
    high_confidence = high_confidence.with_columns(
        [
            pl.col("ingredient_id").map_elements(
                lambda x: f"RXNORM:{x}", return_dtype=pl.Utf8
            ),
            pl.col("effect_meddra_id").map_elements(
                lambda x: f"meddra:{x}", return_dtype=pl.Utf8
            ),
        ]
    )

    vocab_rxnorm_ingredient = vocab_rxnorm_ingredient.filter(
        pl.col("rxnorm_id").str.contains(r"^\d+$")
    ).with_columns(
        [
            pl.col("rxnorm_id").map_elements(
                lambda x: f"RXNORM:{x}", return_dtype=pl.Utf8
            ),
        ]
    )

    vocab_meddra_adverse_effect = vocab_meddra_adverse_effect.with_columns(
        [
            pl.col("meddra_id").map_elements(
                lambda x: f"meddra:{x}", return_dtype=pl.Utf8
            ),
        ]
    )

    return high_confidence.join(
        vocab_rxnorm_ingredient,
        left_on="ingredient_id",
        right_on="rxnorm_id",
        how="inner",
    ).join(
        vocab_meddra_adverse_effect,
        left_on="effect_meddra_id",
        right_on="meddra_id",
        how="inner",
    ).sort(by=["ingredient_id", "effect_meddra_id"])


onsides_node = node(
    run,
    inputs={
        "high_confidence": "landing.onsides.high_confidence",
        "vocab_rxnorm_ingredient": "landing.onsides.vocab_rxnorm_ingredient",
        "vocab_meddra_adverse_effect": "landing.onsides.vocab_meddra_adverse_effect",
    },
    outputs="onsides.high_confidence",
    name="onsides",
    tags=["bronze"],
)
