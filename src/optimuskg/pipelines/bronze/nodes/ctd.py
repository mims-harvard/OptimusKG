from typing import Any, final
import logging

import polars as pl
from typedframe import PolarsTypedFrame, TypedDataFrame
from kedro.pipeline import node

log = logging.getLogger(__name__)


from dataclasses import dataclass
from polars.datatypes import DataType
from typing import Final

@final
class CTDExposureEvents(PolarsTypedFrame):
    schema: Final = {
        "exposurestressorname": str,
        "exposurestressorid": str,
        "stressorsourcecategory": str,
        "stressorsourcedetails": str,
        "numberofstressorsamples": str,
        "stressornotes": str,
        "numberofreceptors": str,
        "receptors": str,
        "receptornotes": str,
        "smokingstatus": str,
        "age": str,
        "ageunitsofmeasurement": str,
        "agequalifier": str,
        "sex": str,
        "race": str,
        "methods": str,
        "detectionlimit": pl.Utf8,
        "detectionlimituom": str,
        "detectionfrequency": pl.Utf8,
        "medium": str,
        "exposuremarker": str,
        "exposuremarkerid": str,
        "markerlevel": str,
        "markerunitsofmeasurement": str,
        "markermeasurementstatistic": str,
        "assaynotes": str,
        "studycountries": str,
        "stateorprovince": str,
        "citytownregionarea": str,
        "exposureeventnotes": str,
        "outcomerelationship": str,
        "diseasename": str,
        "diseaseid": str,
        "phenotypename": str,
        "phenotypeid": str,
        "phenotypeactiondegreetype": str,
        "anatomy": str,
        "exposureoutcomenotes": str,
        "reference": str,
        "associatedstudytitles": str,
        "enrollmentstartyear": str,
        "enrollmentendyear": str,
        "studyfactors": str
    }

def process_cte(
    ctd_exposure_events: pl.DataFrame,
) -> :
    


cte_node = node(
    cte_node,
    {
        "ctd_exposure_events": "landing.ctd.ctd_exposure_events"
    },
    "",
    name="ctd",
)
