import polars as pl
from kedro.pipeline import node

from .utils import classify_age_type, extract_age_value


def run(
    ctd_exposure_events: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    return (
        ctd_exposure_events.filter(pl.col("phenotype_id").is_not_null())
        .with_columns(
            [
                pl.col("phenotype_id").str.replace("GO:", "GO_").alias("phenotype_id"),
            ]
        )
        .join(go_terms, left_on="phenotype_id", right_on="id", how="inner")
        .filter(pl.col("type") == "biological_process")
        .with_columns(
            [
                pl.col("exposure_stressor_id").alias("from"),
                pl.col("phenotype_id").alias("to"),
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
                pl.col("number_of_stressor_samples")
                .cast(pl.Int64)
                .sum()
                .alias("numberOfStressorSamples"),
                pl.col("number_of_receptors")
                .cast(pl.Int64)
                .sum()
                .alias("numberOfReceptors"),
                pl.col("receptors").drop_nulls().unique().alias("receptors"),
                pl.col("receptor_notes").drop_nulls().unique().alias("receptorNotes"),
                pl.col("smoking_status")
                .str.split("|")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .alias("smokingStatuses"),
                pl.col("age_type")
                .filter(pl.col("age_type") != "null")
                .len()
                .alias("ageEntries"),
                pl.when(pl.col("age_type") == "closed_range")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .alias("ageRangeValues"),
                pl.when(pl.col("age_type") == "mean")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .alias("ageMeanValues"),
                pl.when(pl.col("age_type") == "median")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .alias("ageMedianValues"),
                pl.when(pl.col("age_type") == "point")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .alias("agePointValues"),
                pl.when(pl.col("age_type") == "open_range")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .alias("ageOpenRangeValues"),
                pl.col("sex")
                .str.split("|")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .alias("sexes"),
                pl.col("race")
                .str.split("|")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .alias("races"),
                pl.col("methods")
                .str.split("|")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .alias("methods"),
                pl.col("detection_limit").drop_nulls().unique().alias("detectionLimit"),
                pl.col("detection_limit_uom")
                .drop_nulls()
                .unique()
                .alias("detectionLimitUom"),
                pl.col("detection_frequency")
                .drop_nulls()
                .unique()
                .alias("detectionFrequency"),
                pl.col("medium").drop_nulls().unique().alias("mediums"),
                pl.col("assay_notes").drop_nulls().unique().alias("assayNotes"),
                pl.col("study_countries")
                .str.split("|")
                .explode()
                .drop_nulls()
                .unique()
                .alias("studyCountries"),
                pl.col("state_or_province")
                .str.split("|")
                .explode()
                .drop_nulls()
                .unique()
                .alias("statesOrProvinces"),
                pl.col("city_town_region_area")
                .drop_nulls()
                .unique()
                .alias("cityTownRegionAreas"),
                pl.col("exposure_event_notes")
                .drop_nulls()
                .unique()
                .alias("exposureEventNotes"),
                pl.col("outcome_relationship")
                .drop_nulls()
                .unique()
                .alias("outcomeRelationships"),
                pl.col("exposure_outcome_notes")
                .drop_nulls()
                .unique()
                .alias("exposureOutcomeNotes"),
                pl.col("reference").drop_nulls().unique().alias("references"),
                pl.col("associated_study_titles")
                .drop_nulls()
                .unique()
                .alias("associatedStudyTitles"),
                pl.col("enrollment_start_year")
                .drop_nulls()
                .unique()
                .alias("enrollmentStartYears"),
                pl.col("enrollment_end_year")
                .drop_nulls()
                .unique()
                .alias("enrollmentEndYears"),
                pl.col("study_factors")
                .str.split("|")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .alias("studyFactors"),
            ]
        )
        .select(
            [
                pl.col("from"),
                pl.col("to"),
                pl.lit("exposure_biological_process").alias("relation"),
                pl.lit(True).alias("undirected"),
                pl.struct(
                    [
                        pl.lit(["GO", "CTD"]).alias("sources"),
                        pl.lit("interacts with").alias(
                            "relationType"
                        ),  # NOTE: maybe use outcomeRelationships as relationType?
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


exposure_biological_process_node = node(
    run,
    inputs={
        "ctd_exposure_events": "bronze.ctd.ctd_exposure_events",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="exposure_biological_process",
    name="exposure_biological_process",
    tags=["silver"],
)
