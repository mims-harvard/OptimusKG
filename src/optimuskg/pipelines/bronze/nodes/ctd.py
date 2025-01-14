from typing import final
import logging

import polars as pl
from typedframe import PolarsTypedFrame
from kedro.pipeline import node

log = logging.getLogger(__name__)

from typing import Final


@final
class CTDExposureEvents(PolarsTypedFrame):
    schema: Final = {
        "exposurestressorname": pl.String,
        "exposurestressorid": pl.String,
        "stressorsourcecategory": pl.String,
        "stressorsourcedetails": pl.String,
        "numberofstressorsamples": pl.String,
        "stressornotes": pl.String,
        "numberofreceptors": pl.String,
        "receptors": pl.String,
        "receptornotes": pl.String,
        "smokingstatus": pl.String,
        "age": pl.String,
        "ageunitsofmeasurement": pl.String,
        "agequalifier": pl.String,
        "sex": pl.String,
        "race": pl.String,
        "methods": pl.String,
        "detectionlimit": pl.Utf8,
        "detectionlimituom": pl.String,
        "detectionfrequency": pl.Utf8,
        "medium": pl.String,
        "exposuremarker": pl.String,
        "exposuremarkerid": pl.String,
        "markerlevel": pl.String,
        "markerunitsofmeasurement": pl.String,
        "markermeasurementstatistic": pl.String,
        "assaynotes": pl.String,
        "studycountries": pl.String,
        "stateorprovince": pl.String,
        "citytownregionarea": pl.String,
        "exposureeventnotes": pl.String,
        "outcomerelationship": pl.String,
        "diseasename": pl.String,
        "diseaseid": pl.String,
        "phenotypename": pl.String,
        "phenotypeid": pl.String,
        "phenotypeactiondegreetype": pl.String,
        "anatomy": pl.String,
        "exposureoutcomenotes": pl.String,
        "reference": pl.String,
        "associatedstudytitles": pl.String,
        "enrollmentstartyear": pl.String,
        "enrollmentendyear": pl.String,
        "studyfactors": pl.String,
    }


def process_ctd(
    ctd_exposure_events: pl.DataFrame,
) -> pl.DataFrame:
    return CTDExposureEvents(ctd_exposure_events).df


ctd_node = node(
    process_ctd,
    inputs=dict(ctd_exposure_events="landing.ctd.ctd_exposure_events"),
    outputs="ctd.ctd_exposure_events",
    name="ctd",
)
