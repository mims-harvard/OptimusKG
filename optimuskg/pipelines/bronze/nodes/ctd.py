import polars as pl
from kedro.pipeline import node


def run(
    ctd_exposure_events: pl.DataFrame,
) -> pl.DataFrame:
    ctd_exposure_events = ctd_exposure_events.select(
        [
            pl.col("exposurestressorname").alias("exposure_stressor_name"),
            pl.col("exposurestressorid").alias("exposure_stressor_id"),
            pl.col("stressorsourcecategory").alias("stressor_source_category"),
            pl.col("stressorsourcedetails").alias("stressor_source_details"),
            pl.col("numberofstressorsamples").alias("number_of_stressor_samples"),
            pl.col("stressornotes").alias("stressor_notes"),
            pl.col("numberofreceptors").alias("number_of_receptors"),
            pl.col("receptors").alias("receptors"),
            pl.col("receptornotes").alias("receptor_notes"),
            pl.col("smokingstatus").alias("smoking_status"),
            pl.col("age").alias("age"),
            pl.col("ageunitsofmeasurement").alias("age_units_of_measurement"),
            pl.col("agequalifier").alias("age_qualifier"),
            pl.col("sex").alias("sex"),
            pl.col("race").alias("race"),
            pl.col("methods").alias("methods"),
            pl.col("detectionlimit").alias("detection_limit"),
            pl.col("detectionlimituom").alias("detection_limit_uom"),
            pl.col("detectionfrequency").alias("detection_frequency"),
            pl.col("medium").alias("medium"),
            pl.col("exposuremarker").alias("exposure_marker"),
            pl.col("exposuremarkerid").alias("exposure_marker_id"),
            pl.col("markerlevel").alias("marker_level"),
            pl.col("markerunitsofmeasurement").alias("marker_units_of_measurement"),
            pl.col("markermeasurementstatistic").alias("marker_measurement_statistic"),
            pl.col("assaynotes").alias("assay_notes"),
            pl.col("studycountries").alias("study_countries"),
            pl.col("stateorprovince").alias("state_or_province"),
            pl.col("citytownregionarea").alias("city_town_region_area"),
            pl.col("exposureeventnotes").alias("exposure_event_notes"),
            pl.col("outcomerelationship").alias("outcome_relationship"),
            pl.col("diseasename").alias("disease_name"),
            pl.col("diseaseid").alias("disease_id"),
            pl.col("phenotypename").alias("phenotype_name"),
            pl.col("phenotypeid").alias("phenotype_id"),
            pl.col("phenotypeactiondegreetype").alias("phenotype_action_degree_type"),
            pl.col("anatomy").alias("anatomy"),
            pl.col("exposureoutcomenotes").alias("exposure_outcome_notes"),
            pl.col("reference").alias("reference"),
            pl.col("associatedstudytitles").alias("associated_study_titles"),
            pl.col("enrollmentstartyear").alias("enrollment_start_year"),
            pl.col("enrollmentendyear").alias("enrollment_end_year"),
            pl.col("studyfactors").alias("study_factors"),
        ]
    )

    ctd_exposure_events = ctd_exposure_events.with_columns(
        [
            pl.col("exposure_marker_id").map_elements(
                lambda x: f"MESH:{x}",
                return_dtype=pl.Utf8,
            ),
            pl.col("exposure_stressor_id").map_elements(
                lambda x: f"MESH:{x}",
                return_dtype=pl.Utf8,
            ),
            pl.col("disease_id").map_elements(
                lambda x: f"MESH:{x}",
                return_dtype=pl.Utf8,
            ),
        ]
    )

    return ctd_exposure_events.sort(by="exposure_stressor_id")


ctd_node = node(
    run,
    inputs={"ctd_exposure_events": "landing.ctd.ctd_exposure_events"},
    outputs="ctd.ctd_exposure_events",
    name="ctd",
    tags=["bronze"],
)
