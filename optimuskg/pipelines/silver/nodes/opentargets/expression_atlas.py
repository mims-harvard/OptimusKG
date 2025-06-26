import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import construct_edges


def process_expression_atlas(  # noqa: PLR0913
    expression_atlas: pd.DataFrame,
    phenotypes: pl.DataFrame,
    diseases: pl.DataFrame,
    targets: pl.DataFrame,
    drug_mappings: pl.DataFrame,
) -> pl.DataFrame:
    df = pl.from_pandas(expression_atlas)
    df = (
        df.select(
            [
                "id",
                "targetId",
                "diseaseId",
                "studyId",
                "contrast",
                "log2FoldChangeValue",
                "log2FoldChangePercentileRank",
                "score",
            ]
        )
        .filter(pl.col("targetId").is_in(targets["id"]))
        .filter(pl.col("diseaseId").is_in(diseases["id"]))
        .select(["targetId", "diseaseId", "log2FoldChangeValue"])
        .group_by(["targetId", "diseaseId"])
        .agg(pl.col("log2FoldChangeValue").mean())
        .filter(pl.col("log2FoldChangeValue").abs() > 1)
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


expression_atlas_node = node(
    process_expression_atlas,
    inputs={
        "expression_atlas": "bronze.opentargets.evidence.expression_atlas",
        "phenotypes": "bronze.ontology.phenotypes",
        "diseases": "bronze.opentargets.diseases",
        "targets": "bronze.opentargets.targets",
        "drug_mappings": "bronze.opentargets.drug_mappings",
    },
    outputs="opentargets.evidence.expression_atlas",
    name="expression_atlas",
    tags=["silver"],
)
