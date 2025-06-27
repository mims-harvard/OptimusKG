import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import construct_edges


def run(  # noqa: PLR0913
    crispr_screen: pd.DataFrame,
    phenotypes: pl.DataFrame,
    diseases: pl.DataFrame,
    targets: pl.DataFrame,
    drug_mappings: pl.DataFrame,
    score_threshold: float,
) -> pl.DataFrame:
    df = pl.from_pandas(crispr_screen)
    df = (
        df.select(
            [
                "id",
                "targetId",
                "diseaseId",
                "targetFromSourceId",
                "diseaseFromSource",
                "score",
                "log2FoldChangeValue",
                "cellType",
                "crisprScreenLibrary",
            ]
        )
        .filter(pl.col("targetId").is_in(targets["id"]))
        .filter(pl.col("diseaseId").is_in(diseases["id"]))
        .select(["score", "log2FoldChangeValue", "targetId", "diseaseId"])
        .group_by(["targetId", "diseaseId"])
        .agg(pl.col("score").mean(), pl.col("log2FoldChangeValue").mean())
        .filter(pl.col("score") > score_threshold)
        .filter(pl.col("log2FoldChangeValue") != 0)
        .with_columns(
            pl.when(pl.col("log2FoldChangeValue") < 0)
            .then(pl.lit("disease_protein_negative"))
            .otherwise(pl.lit("disease_protein_positive"))
            .alias("relation"),
            pl.when(pl.col("log2FoldChangeValue") < 0)
            .then(pl.lit("expression downregulated"))
            .otherwise(pl.lit("expression upregulated"))
            .alias("relation_type"),
        )
        .pipe(
            construct_edges,
            targets_df=pl.DataFrame(targets),
            phenotypes_df=pl.DataFrame(phenotypes),
            diseases_df=diseases,
            drug_mappings_df=drug_mappings,
            type_x="gene",
            type_y="disease",
            relation_label=None,
            display_relation_label=None,
        )
    )

    df = df.sort(by=sorted(df.columns))
    return df


crispr_screen_node = node(
    run,
    inputs={
        "crispr_screen": "bronze.opentargets.evidence.crispr_screen",
        "phenotypes": "bronze.ontology.phenotypes",
        "diseases": "bronze.opentargets.diseases",
        "targets": "bronze.opentargets.targets",
        "drug_mappings": "bronze.opentargets.drug_mappings",
        "score_threshold": "params:score_threshold",
    },
    outputs="opentargets.evidence.crispr_screen",
    name="crispr_screen",
    tags=["silver"],
)
