import logging
from collections.abc import Callable
from typing import Any, Final, final

import polars as pl
from kedro.pipeline import node
from typedframe import PolarsTypedFrame

from optimuskg.utils import to_snake_case

logger = logging.getLogger(__name__)


@final
class TargetDiseaseEvidenceSchema(PolarsTypedFrame):
    # NOTE: this schema is not in the website (https://platform.opentargets.org/downloads)
    # But assumed from reading all the evidence datasets
    schema: Final[dict[str, type[pl.DataType] | pl.List]] = {
        "datasourceId": pl.String,
        "targetId": pl.String,
        "alleleOrigins": pl.List(pl.String),
        "allelicRequirements": pl.List(pl.String),
        "ancestry": pl.String,
        "ancestryId": pl.String,
        "beta": pl.Float64,
        "betaConfidenceIntervalLower": pl.Float64,
        "betaConfidenceIntervalUpper": pl.Float64,
        "biologicalModelAllelicComposition": pl.String,
        "biologicalModelGeneticBackground": pl.String,
        "biologicalModelId": pl.String,
        "biomarkerName": pl.String,
        "biomarkers": pl.String,
        "biosamplesFromSource": pl.List(pl.String),
        "cellType": pl.String,
        "clinicalPhase": pl.Float64,
        "clinicalSignificances": pl.String,
        "clinicalStatus": pl.String,
        "cohortDescription": pl.String,
        "cohortId": pl.String,
        "cohortPhenotypes": pl.List(pl.String),
        "cohortShortName": pl.String,
        "confidence": pl.String,
        "contrast": pl.String,
        "crisprScreenLibrary": pl.String,
        "datatypeId": pl.String,
        "diseaseCellLines": pl.List(
            pl.Struct(
                {
                    "id": pl.String,
                    "name": pl.String,
                    "tissue": pl.String,
                    "tissueId": pl.String,
                }
            )
        ),
        "diseaseFromSource": pl.String,
        "diseaseFromSourceId": pl.String,
        "diseaseFromSourceMappedId": pl.String,
        "diseaseModelAssociatedHumanPhenotypes": pl.String,
        "diseaseModelAssociatedModelPhenotypes": pl.String,
        "drugFromSource": pl.String,
        "drugId": pl.String,
        "drugResponse": pl.String,
        "geneticBackground": pl.String,
        "literature": pl.List(pl.String),
        "log2FoldChangePercentileRank": pl.Float64,
        "log2FoldChangeValue": pl.Float64,
        "mutatedSamples": pl.List(
            pl.Struct(
                {
                    "functionalConsequenceId": pl.String,
                    "numberMutatedSamples": pl.Float64,
                    "numberSamplesTested": pl.Float64,
                    "numberSamplesWithMutationType": pl.Int64,
                }
            )
        ),
        "oddsRatio": pl.Float64,
        "oddsRatioConfidenceIntervalLower": pl.Float64,
        "oddsRatioConfidenceIntervalUpper": pl.Float64,
        "pValueExponent": pl.Float64,
        "pValueMantissa": pl.Float64,
        "pathways": pl.List(
            pl.Struct(
                {
                    "id": pl.String,
                    "name": pl.String,
                }
            )
        ),
        "pmcIds": pl.String,
        "projectId": pl.String,
        "publicationFirstAuthor": pl.String,
        "publicationYear": pl.Float64,
        "reactionId": pl.String,
        "reactionName": pl.String,
        "releaseDate": pl.String,
        "releaseVersion": pl.String,
        "resourceScore": pl.Float64,
        "sex": pl.List(pl.String),
        "significantDriverMethods": pl.List(pl.String),
        "statisticalMethod": pl.String,
        "statisticalMethodOverview": pl.String,
        "statisticalTestTail": pl.String,
        "studyCases": pl.Float64,
        "studyCasesWithQualifyingVariants": pl.Float64,
        "studyId": pl.String,
        "studyOverview": pl.String,
        "studySampleSize": pl.Float64,
        "studyStartDate": pl.String,
        "studyStopReason": pl.String,
        "studyStopReasonCategories": pl.List(pl.String),
        "targetFromSource": pl.String,
        "targetFromSourceId": pl.String,
        "targetInModel": pl.String,
        "targetInModelEnsemblId": pl.String,
        "targetInModelMgiId": pl.String,
        "targetModulation": pl.String,
        "textMiningSentences": pl.String,
        "urls": pl.List(
            pl.Struct(
                {
                    "niceName": pl.String,
                    "url": pl.String,
                }
            )
        ),
        "variantAminoacidDescriptions": pl.List(pl.String),
        "variantFunctionalConsequenceFromQtlId": pl.String,
        "variantFunctionalConsequenceId": pl.String,
        "variantHgvsId": pl.String,
        "variantId": pl.String,
        "variantRsId": pl.String,
        "diseaseId": pl.String,
        "id": pl.String,
        "score": pl.Float64,
        "variantEffect": pl.String,
        "directionOnTrait": pl.String,
    }


def concat_partitions(
    partitioned_input: dict[str, Callable[[], Any]],
) -> pl.DataFrame:
    """Concatenate input partitions into one Polars DataFrame.

    Args:
        partitioned_input: A dictionary with partition ids as keys and load functions as values.

    Returns:
        Polars DataFrame representing a concatenation of all loaded partitions.
    """
    partitions = []

    for _, partition_load_func in sorted(partitioned_input.items()):
        partition_data = partition_load_func()
        pl_data = pl.from_pandas(partition_data)

        # Standardize sex column to pl.List(pl.String)
        if "sex" in pl_data.columns:
            pl_data = pl_data.with_columns(
                pl.col("sex").map_elements(
                    lambda x: [x] if isinstance(x, str) else x,
                    return_dtype=pl.List(pl.String),
                )
            )
        partitions.append(pl_data)

    concated_df = pl.concat(partitions, how="vertical")
    assert isinstance(concated_df, pl.DataFrame)
    return concated_df


def run(  # noqa: PLR0913
    cancer_gene_census: dict[str, Callable[[], Any]],
    chembl: dict[str, Callable[[], Any]],
    clingen: dict[str, Callable[[], Any]],
    crispr: dict[str, Callable[[], Any]],
    crispr_screen: dict[str, Callable[[], Any]],
    expression_atlas: dict[str, Callable[[], Any]],
    gene_burden: dict[str, Callable[[], Any]],
    gene2phenotype: dict[str, Callable[[], Any]],
    genomics_england: dict[str, Callable[[], Any]],
    intogen: dict[str, Callable[[], Any]],
    orphanet: dict[str, Callable[[], Any]],
    progeny: dict[str, Callable[[], Any]],
    reactome: dict[str, Callable[[], Any]],
    slapenrich: dict[str, Callable[[], Any]],
    sysbio: dict[str, Callable[[], Any]],
    uniprot_literature: dict[str, Callable[[], Any]],
) -> pl.DataFrame:
    merged_df = pl.concat(
        [
            TargetDiseaseEvidenceSchema.convert(
                df=concat_partitions(cancer_gene_census)
            ).df,
            TargetDiseaseEvidenceSchema.convert(df=concat_partitions(chembl)).df,
            TargetDiseaseEvidenceSchema.convert(df=concat_partitions(clingen)).df,
            TargetDiseaseEvidenceSchema.convert(df=concat_partitions(crispr)).df,
            TargetDiseaseEvidenceSchema.convert(df=concat_partitions(crispr_screen)).df,
            TargetDiseaseEvidenceSchema.convert(
                df=concat_partitions(expression_atlas)
            ).df,
            TargetDiseaseEvidenceSchema.convert(df=concat_partitions(gene_burden)).df,
            TargetDiseaseEvidenceSchema.convert(
                df=concat_partitions(gene2phenotype)
            ).df,
            TargetDiseaseEvidenceSchema.convert(
                df=concat_partitions(genomics_england)
            ).df,
            TargetDiseaseEvidenceSchema.convert(df=concat_partitions(intogen)).df,
            TargetDiseaseEvidenceSchema.convert(df=concat_partitions(orphanet)).df,
            TargetDiseaseEvidenceSchema.convert(df=concat_partitions(progeny)).df,
            TargetDiseaseEvidenceSchema.convert(df=concat_partitions(reactome)).df,
            TargetDiseaseEvidenceSchema.convert(df=concat_partitions(slapenrich)).df,
            TargetDiseaseEvidenceSchema.convert(df=concat_partitions(sysbio)).df,
            TargetDiseaseEvidenceSchema.convert(
                df=concat_partitions(uniprot_literature)
            ).df,
        ]
    )
    df = merged_df.select(
        [
            "datasourceId",
            "targetId",
            "diseaseId",
            "drugId",
            "drugResponse",
            "ancestryId",
            "betaConfidenceIntervalLower",
            "betaConfidenceIntervalUpper",
            "cellType",
            "clinicalPhase",
            "clinicalStatus",
            "confidence",
            "contrast",
            "crisprScreenLibrary",
            "datatypeId",
            "diseaseFromSourceMappedId",
            "geneticBackground",
            "log2FoldChangePercentileRank",
            "log2FoldChangeValue",
            "oddsRatio",
            "oddsRatioConfidenceIntervalLower",
            "oddsRatioConfidenceIntervalUpper",
            "pValueExponent",
            "pValueMantissa",
            "projectId",
            "publicationFirstAuthor",
            "publicationYear",
            "reactionId",
            "reactionName",
            "releaseDate",
            "releaseVersion",
            "resourceScore",
            "statisticalMethod",
            "statisticalMethodOverview",
            "studyCases",
            "studyId",
            "studyOverview",
            "studySampleSize",
            "studyStartDate",
            "studyStopReason",
            "targetFromSource",
            "targetFromSourceId",
            "score",
            "variantEffect",
            "directionOnTrait",
        ]
    )

    return df.rename({col: to_snake_case(col) for col in df.columns}).unique()


evidence_node = node(
    run,
    inputs={
        "cancer_gene_census": "landing.opentargets.evidence.cancer_gene_census",
        "chembl": "landing.opentargets.evidence.chembl",
        "clingen": "landing.opentargets.evidence.clingen",
        "crispr": "landing.opentargets.evidence.crispr",
        "crispr_screen": "landing.opentargets.evidence.crispr_screen",
        "expression_atlas": "landing.opentargets.evidence.expression_atlas",
        "gene_burden": "landing.opentargets.evidence.gene_burden",
        "gene2phenotype": "landing.opentargets.evidence.gene2phenotype",
        "genomics_england": "landing.opentargets.evidence.genomics_england",
        "intogen": "landing.opentargets.evidence.intogen",
        "orphanet": "landing.opentargets.evidence.orphanet",
        "progeny": "landing.opentargets.evidence.progeny",
        "reactome": "landing.opentargets.evidence.reactome",
        "slapenrich": "landing.opentargets.evidence.slapenrich",
        "sysbio": "landing.opentargets.evidence.sysbio",
        "uniprot_literature": "landing.opentargets.evidence.uniprot_literature",
    },
    outputs="opentargets.evidence",
    name="evidence",
    tags=["bronze"],
)
