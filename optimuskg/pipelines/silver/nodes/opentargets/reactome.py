import pandas as pd
import polars as pl
from kedro.pipeline import node

from .utils import construct_edges


def process_reactome(  # noqa: PLR0913
    reactome: pd.DataFrame,
    phenotypes: pl.DataFrame,
    diseases: pl.DataFrame,
    targets: pl.DataFrame,
    drug_mappings: pl.DataFrame,
    disease_phenotype_ids: pl.DataFrame,
) -> pl.DataFrame:
    df = pl.from_pandas(reactome)
    df = (
        df.select(
            [
                "id",
                "targetId",
                "diseaseId",
                "targetFromSourceId",
                "diseaseFromSource",
                "targetModulation",
                "reactionId",
                "reactionName",
                "score",
            ]
        )
        .filter(pl.col("targetId").is_in(targets["id"]))
        .filter(pl.col("diseaseId").is_in(disease_phenotype_ids["id"]))
        .pipe(
            construct_edges,
            targets_df=pl.DataFrame(targets),
            phenotypes_df=pl.DataFrame(phenotypes),
            diseases_df=diseases,
            drug_mappings_df=drug_mappings,
            type_x="gene",
            type_y="disease",
            relation_label="disease_protein",
            display_relation_label="associated with",
        )
    )

    df = df.sort(by=sorted(df.columns))
    return df


ot__reactome_node = node(
    process_reactome,
    inputs={
        "reactome": "bronze.opentargets.evidence.reactome",
        "phenotypes": "bronze.opentargets.phenotypes",
        "diseases": "bronze.opentargets.diseases",
        "targets": "bronze.opentargets.targets",
        "disease_phenotype_ids": "bronze.opentargets.disease_phenotype_ids",
        "drug_mappings": "bronze.opentargets.drug_mappings",
    },
    outputs="opentargets.evidence.reactome",
    name="ot__reactome",  # TODO: add "ot__" prefix to all opentargets nodes (in bronze and silver) to avoid clashing with other names
)
