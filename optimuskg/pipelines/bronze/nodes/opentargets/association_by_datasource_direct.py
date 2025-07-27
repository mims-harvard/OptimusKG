from collections.abc import Callable
from typing import Any, Final

import polars as pl
from kedro.pipeline import node
from typedframe import PolarsTypedFrame

from .utils import concat_partitions


class AssociationByDatasourceDirectSchema(PolarsTypedFrame):
    schema: Final[dict[str, type[pl.DataType] | pl.List]] = {
        "datatypeId": pl.String,
        "datasourceId": pl.String,
        "diseaseId": pl.String,
        "targetId": pl.String,
        "score": pl.Float64,
        "evidenceCount": pl.Int64,
    }


def run(
    associations: dict[str, Callable[[], Any]],
) -> pl.DataFrame:
    return AssociationByDatasourceDirectSchema.convert(
        concat_partitions(associations)
    ).df


association_by_datasource_direct_node = node(
    run,
    inputs={"associations": "landing.opentargets.association_by_datasource_direct"},
    outputs="opentargets.association_by_datasource_direct",
    name="association_by_datasource_direct",
    tags=["bronze"],
)
