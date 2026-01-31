import polars as pl
from kedro.pipeline import node

from .utils import classify_age_type, extract_age_value


def run(
    ctd_exposure_events: pl.DataFrame,
    mondo_xrefs: pl.DataFrame,
) -> pl.DataFrame:
    return (
        ctd_exposure_events.filter(pl.col("disease_id").is_not_null())
        .join(mondo_xrefs, left_on="disease_id", right_on="xref_id", how="inner")
        .with_columns(
            [
                pl.col("exposure_stressor_id").alias("from"),
                pl.col("id").alias("to"),
                # Classify age types
                pl.struct(["age", "age_qualifier"])
                .map_elements(
                    lambda row: classify_age_type(
                        row["age"],
                        row["age_qualifier"],
                    ),
                    return_dtype=pl.String,
                )
                .alias("age_type"),
                # Extract numeric values
                pl.struct(["age", "age_units_of_measurement"])
                .map_elements(
                    lambda row: extract_age_value(
                        row["age"],
                        row["age_units_of_measurement"],
                    ),
                    return_dtype=pl.Float64,
                )
                .alias("age_value_years"),
            ]
        )
        .group_by(["from", "to"])
        .agg(
            [
                pl.len().alias("evidenceCount"),
                pl.col("number_of_receptors")
                .cast(pl.Int64)
                .sum()
                .alias("numberOfReceptors"),
                pl.col("receptors").drop_nulls().unique().sort().alias("receptors"),
                pl.col("receptor_notes").drop_nulls().unique().sort().alias("receptorNotes"),
                pl.col("smoking_status")
                .str.split("|")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .sort()
                .alias("smokingStatuses"),
                pl.col("age_type")
                .filter(pl.col("age_type") != "null")
                .len()
                .alias("ageEntries"),
                pl.when(pl.col("age_type") == "closed_range")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .sort()
                .alias("ageRangeValues"),
                pl.when(pl.col("age_type") == "mean")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .sort()
                .alias("ageMeanValues"),
                pl.when(pl.col("age_type") == "median")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .sort()
                .alias("ageMedianValues"),
                pl.when(pl.col("age_type") == "point")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .sort()
                .alias("agePointValues"),
                pl.when(pl.col("age_type") == "open_range")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .sort()
                .alias("ageOpenRangeValues"),
                pl.col("sex")
                .str.split("|")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .sort()
                .alias("sexes"),
                pl.col("race")
                .str.split("|")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .sort()
                .alias("races"),
                pl.col("methods")
                .str.split("|")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .sort()
                .alias("methods"),
                pl.col("detection_limit").drop_nulls().unique().sort().alias("detectionLimit"),
                pl.col("detection_limit_uom")
                .drop_nulls()
                .unique()
                .sort()
                .alias("detectionLimitUom"),
                pl.col("detection_frequency")
                .drop_nulls()
                .unique()
                .sort()
                .alias("detectionFrequency"),
                pl.col("medium").drop_nulls().unique().sort().alias("mediums"),
                pl.col("assay_notes")
                .drop_nulls()
                .unique()
                .sort()
                .alias("assayNotes"),  # this column is new
                pl.col("study_countries")
                .str.split("|")
                .explode()
                .drop_nulls()
                .unique()
                .sort()
                .alias("studyCountries"),
                pl.col("state_or_province")
                .str.split("|")
                .explode()
                .drop_nulls()
                .unique()
                .sort()
                .alias("statesOrProvinces"),
                pl.col("city_town_region_area")
                .str.split("|")
                .explode()
                .drop_nulls()
                .unique()
                .sort()
                .alias("cityTownRegionAreas"),
                pl.col("exposure_event_notes")
                .drop_nulls()
                .unique()
                .sort()
                .alias("exposureEventNotes"),
                pl.col("outcome_relationship")
                .drop_nulls()
                .unique()
                .sort()
                .alias("outcomeRelationships"),
                pl.col("exposure_outcome_notes")
                .drop_nulls()
                .unique()
                .sort()
                .alias("exposureOutcomeNotes"),
                pl.col("reference").drop_nulls().unique().sort().alias("references"),
                pl.col("associated_study_titles")
                .drop_nulls()
                .unique()
                .sort()
                .alias("associatedStudyTitles"),
                pl.col("enrollment_start_year")
                .drop_nulls()
                .unique()
                .sort()
                .alias("enrollmentStartYears"),
                pl.col("enrollment_end_year")
                .drop_nulls()
                .unique()
                .sort()
                .alias("enrollmentEndYears"),
                pl.col("study_factors")
                .str.split("|")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .sort()
                .alias("studyFactors"),
            ]
        )
        .select(
            [
                pl.col("from"),
                pl.col("to"),
                pl.lit("exposure_disease").alias("relation"),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.lit(["CTD", "MONDO"]).alias("sources"),
                        pl.lit("linked to").alias("relationType"),
                        pl.col("evidenceCount"),
                        pl.col("numberOfReceptors"),
                        pl.col("receptors"),
                        pl.col("receptorNotes"),
                        pl.col("smokingStatuses"),
                        pl.col("ageEntries"),
                        pl.col("ageRangeValues"),
                        pl.col("ageMeanValues"),
                        pl.col("ageMedianValues"),
                        pl.col("agePointValues"),
                        pl.col("ageOpenRangeValues"),
                        pl.col("sexes"),
                        pl.col("races"),
                        pl.col("methods"),
                        pl.col("detectionLimit"),
                        pl.col("detectionLimitUom"),
                        pl.col("detectionFrequency"),
                        pl.col("mediums"),
                        pl.col("assayNotes"),
                        pl.col("studyCountries"),
                        pl.col("statesOrProvinces"),
                        pl.col("cityTownRegionAreas"),
                        pl.col("exposureEventNotes"),
                        pl.col("outcomeRelationships"),
                        pl.col("exposureOutcomeNotes"),
                        pl.col("references"),
                        pl.col("associatedStudyTitles"),
                        pl.col("enrollmentStartYears"),
                        pl.col("enrollmentEndYears"),
                        pl.col("studyFactors"),
                    ]
                ).alias("properties"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


exposure_disease_node = node(
    run,
    inputs={
        "ctd_exposure_events": "bronze.ctd.ctd_exposure_events",
        "mondo_xrefs": "bronze.ontology.mondo_xrefs",
    },
    outputs="edges.exposure_disease",
    name="exposure_disease",
    tags=["silver"],
)
