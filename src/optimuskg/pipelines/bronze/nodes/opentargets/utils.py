import logging
from collections.abc import Callable
from typing import Any, Final, final

import pandas as pd
import polars as pl
from typedframe import PolarsTypedFrame

log = logging.getLogger(__name__)


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


# @final
# class TargetSchema(PolarsTypedFrame):
#     schema: Final[dict[str, type[pl.DataType] | pl.List]] = {
#         "id": pl.String,
#         "approvedSymbol": pl.String,
#         "biotype": pl.String,
#         "transcriptIds": pl.List(pl.String),
#         "canonicalTranscript": pl.List(
#             pl.Struct(
#                 {
#                     "id": pl.String,
#                     "chromosome": pl.String,
#                     "start": pl.Int64,
#                     "end": pl.Int64,
#                     "strand": pl.String,
#                 }
#             )
#         ),
#         "canonicalExons": pl.List(pl.String),
#         "genomicLocation": pl.List(
#             pl.Struct(
#                 {
#                     "chromosome": pl.String,
#                     "start": pl.Int64,
#                     "end": pl.Int64,
#                     "strand": pl.Int64,
#                 }
#             )
#         ),
#         "alternativeGenes": pl.List(pl.String),
#         "approvedName": pl.String,
#         "go": pl.List(
#             pl.Struct(
#                 {
#                     "id": pl.String,
#                     "source": pl.String,
#                     "evidence": pl.String,
#                     "aspect": pl.String,
#                     "geneProduct": pl.String,
#                     "ecoId": pl.String,
#                 }
#             )
#         ),
#         "hallmarks": pl.List(
#             pl.Struct(
#                 {
#                     "attributes": pl.List(
#                         pl.Struct(
#                             {
#                                 "pmid": pl.Int64,
#                                 "description": pl.String,
#                                 "attribute_name": pl.String,
#                             }
#                         )
#                     ),
#                     "cancerHallmarks": pl.List(
#                         pl.Struct(
#                             {
#                                 "pmid": pl.Int64,
#                                 "description": pl.String,
#                                 "impact": pl.String,
#                                 "label": pl.String,
#                             }
#                         )
#                     ),
#                 }
#             )
#         ),
#         "synonyms": pl.List(pl.Struct({"label": pl.String, "source": pl.String})),
#         "symbolSynonyms": pl.List(pl.Struct({"label": pl.String, "source": pl.String})),
#         "nameSynonyms": pl.List(pl.Struct({"label": pl.String, "source": pl.String})),
#         "functionDescriptions": pl.List(pl.String),
#         "subcellularLocations": pl.List(
#             pl.Struct(
#                 {
#                     "location": pl.String,
#                     "source": pl.String,
#                     "termSL": pl.String,
#                     "labelSL": pl.String,
#                 }
#             )
#         ),
#         "targetClass": pl.List(
#             pl.Struct(
#                 {
#                     "id": pl.Int64,
#                     "label": pl.String,
#                     "level": pl.String,
#                 }
#             )
#         ),
#         "obsoleteSymbols": pl.List(
#             pl.Struct({"label": pl.String, "source": pl.String})
#         ),
#         "obsoleteNames": pl.List(pl.Struct({"label": pl.String, "source": pl.String})),
#         "constraint": pl.List(
#             pl.Struct(
#                 {
#                     "constraintType": pl.String,
#                     "score": pl.Float64,
#                     "exp": pl.Float64,
#                     "obs": pl.Int64,
#                     "oe": pl.Float64,
#                     "oeLower": pl.Float64,
#                     "oeUpper": pl.Float64,
#                     "upperRank": pl.Int64,
#                     "upperBin": pl.Int64,
#                     "upperBin6": pl.Int64,
#                 }
#             )
#         ),
#         "tep": pl.List(
#             pl.Struct(
#                 {
#                     "targetFromSourceId": pl.String,
#                     "description": pl.String,
#                     "therapeuticArea": pl.String,
#                     "url": pl.String,
#                 }
#             )
#         ),
#         "proteinIds": pl.List(pl.Struct({"id": pl.String, "source": pl.String})),
#         "dbXrefs": pl.List(pl.Struct({"id": pl.String, "source": pl.String})),
#         "chemicalProbes": pl.List(
#             pl.Struct(
#                 {
#                     "control": pl.String,
#                     "drugId": pl.String,
#                     "id": pl.String,
#                     "isHighQuality": pl.Boolean,
#                     "mechanismOfAction": pl.List(pl.String),
#                     "origin": pl.List(pl.String),
#                     "probeMinerScore": pl.Int64,
#                     "probesDrugsScore": pl.Int64,
#                     "scoreInCells": pl.Int64,
#                     "scoreInOrganisms": pl.Int64,
#                     "targetFromSourceId": pl.String,
#                     "urls": pl.List(
#                         pl.Struct(
#                             {
#                                 "niceName": pl.String,
#                                 "url": pl.String,
#                             }
#                         )
#                     ),
#                 }
#             )
#         ),
#         "homologues": pl.List(
#             pl.Struct(
#                 {
#                     "speciesId": pl.String,
#                     "speciesName": pl.String,
#                     "homologyType": pl.String,
#                     "targetGeneId": pl.String,
#                     "isHighConfidence": pl.String,
#                     "targetGeneSymbol": pl.String,
#                     "queryPercentageIdentity": pl.Float64,
#                     "targetPercentageIdentity": pl.Float64,
#                     "priority": pl.Int64,
#                 }
#             )
#         ),
#         "tractability": pl.List(
#             pl.Struct(
#                 {
#                     "modality": pl.String,
#                     "id": pl.String,
#                     "value": pl.Boolean,
#                 }
#             )
#         ),
#         "safetyLiabilities": pl.List(
#             pl.Struct(
#                 {
#                     "event": pl.String,
#                     "eventId": pl.String,
#                     "effects": pl.List(
#                         pl.Struct(
#                             {
#                                 "direction": pl.String,
#                                 "dosing": pl.String,
#                             }
#                         )
#                     ),
#                     "biosamples": pl.List(
#                         pl.Struct(
#                             {
#                                 "cellFormat": pl.String,
#                                 "cellLabel": pl.String,
#                                 "tissueId": pl.String,
#                                 "tissueLabel": pl.String,
#                             }
#                         )
#                     ),
#                     "isHumanApplicable": pl.Boolean,
#                     "datasource": pl.String,
#                     "literature": pl.String,
#                     "url": pl.String,
#                     "studies": pl.List(
#                         pl.Struct(
#                             {
#                                 "description": pl.String,
#                                 "name": pl.String,
#                                 "type": pl.String,
#                             }
#                         )
#                     ),
#                 }
#             )
#         ),
#         "pathways": pl.List(
#             pl.Struct(
#                 {
#                     "pathwayId": pl.String,
#                     "pathway": pl.String,
#                     "topLevelTerm": pl.String,
#                 }
#             )
#         ),
#     }


@final
class KGNodeSchema(PolarsTypedFrame):
    schema: Final[dict[str, type[pl.DataType]]] = {
        "node_index": pl.String,
        "node_id": pl.String,
        "node_type": pl.String,
        "node_name": pl.String,
        "node_source": pl.String,
    }


@final
class KGEdgeSchema(PolarsTypedFrame):
    schema: Final[dict[str, type[pl.DataType]]] = {
        "relation": pl.String,
        "display_relation": pl.String,
        "x_index": pl.String,
        "y_index": pl.String,
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

    for partition_key, partition_load_func in sorted(partitioned_input.items()):
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


def concat_json_partitions(
    partitioned_input: dict[str, Callable[[], Any]],
) -> pl.DataFrame:
    """
    Concatenate JSON partitions while enforcing a consistent schema.

    Args:
        partitioned_input: Dictionary mapping partition keys to loader functions
        target_schema: Target schema to enforce on all partitions

    Returns:
        Concatenated DataFrame with enforced schema
    """
    partitions = []

    for partition_key, partition_load_func in sorted(partitioned_input.items()):
        partition_data = partition_load_func()
        partitions.append(partition_data)

    concated_df = pd.concat(partitions)
    concated_df = pl.from_pandas(concated_df)
    assert isinstance(concated_df, pl.DataFrame)
    return concated_df
