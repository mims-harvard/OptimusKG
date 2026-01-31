import polars as pl
from kedro.pipeline import node

from .utils import classify_age_type, extract_age_value


def run(
    ctd_exposure_events: pl.DataFrame,
    target: pl.DataFrame,
) -> pl.DataFrame:
    return (
        ctd_exposure_events.filter(
            # exposure_marker_id is a MeSH or a NCBI Gene identifier (when the ID is only a number). see: https://ctdbase.org/downloads/#exposureevents
            # we filter the Genes to later map them by their symbol to get the Ensembl ID
            pl.col("exposure_marker_id").str.contains(r"^MESH:\d+$")
        )
        .join(
            target.select("id", "approved_symbol"),
            left_on="exposure_marker",
            right_on="approved_symbol",
            how="inner",
        )
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
                pl.col("number_of_stressor_samples")
                .cast(pl.Int64)
                .sum()
                .alias("numberOfStressorSamples"),
                # TODO: we could add the stressor_notes column
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
                pl.col("assay_notes").drop_nulls().unique().sort().alias("assayNotes"),
                # TODO: we can add this columns with a similar approach of the ages columns: marker_level, marker_units_of_measurement, marker_measurement_statistic
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
                pl.lit("exposure_protein").alias("relation"),
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.lit(["CTD", "opentargets"]).alias("sources"),
                        pl.lit("interacts with").alias(
                            "relationType"
                        ),  # NOTE: this is the same relation type used in PrimeKG, but we need to validate if this is consistent with the ontology tree.
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


exposure_protein_node = node(
    run,
    inputs={
        "ctd_exposure_events": "bronze.ctd.ctd_exposure_events",
        "target": "bronze.opentargets.target",
    },
    outputs="edges.exposure_protein",
    name="exposure_protein",
    tags=["silver"],
)
