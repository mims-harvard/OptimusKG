from collections.abc import Callable
from typing import Any, Final, final

import polars as pl
from kedro.pipeline import node
from typedframe import PolarsTypedFrame

from .utils import concat_json_partitions


@final
class DiseaseSchema(PolarsTypedFrame):
    schema: Final[dict[str, type[pl.DataType] | pl.List | pl.Struct]] = {
        "id": pl.String,
        "code": pl.String,
        "dbXRefs": pl.List(pl.String),
        "description": pl.String,
        "name": pl.String,
        "parents": pl.List(pl.String),
        "synonyms": pl.Struct(
            {
                "hasBroadSynonym": pl.List(pl.String),
                "hasExactSynonym": pl.List(pl.String),
                "hasNarrowSynonym": pl.List(pl.String),
                "hasRelatedSynonym": pl.List(pl.String),
            }
        ),
        "ancestors": pl.List(pl.String),
        "descendants": pl.List(pl.String),
        "children": pl.List(pl.String),
        "therapeuticAreas": pl.List(pl.String),
        "ontology": pl.Struct(
            {
                "isTherapeuticArea": pl.Boolean,
                "leaf": pl.Boolean,
                "sources": pl.Struct({"name": pl.String, "url": pl.String}),
            }
        ),
        "obsoleteTerms": pl.List(pl.String),
        "indirectLocationIds": pl.List(pl.String),
        "directLocationIds": pl.List(pl.String),
    }


def process_diseases(
    diseases: dict[str, Callable[[], Any]],
    mondo_efo_mappings_df: pl.DataFrame,
) -> pl.DataFrame:
    concated_df = concat_json_partitions(diseases)
    df = DiseaseSchema.convert(concated_df).df

    df = df.select("id", "name", "description")

    df = df.join(
        mondo_efo_mappings_df,
        left_on="id",
        right_on="efo_id",
        how="left",
    )
    df = df.with_columns(
        [
            pl.when(pl.col("mondo_id").is_not_null())
            .then(pl.col("mondo_id"))
            .otherwise(pl.col("id"))
            .alias("combined_id")
        ]
    )

    # Drop and rename columns
    df = df.drop("mondo_id")
    df = df.rename({"combined_id": "mondo_id"})

    # Filter MONDO entries and create node_id
    df = df.filter(pl.col("mondo_id").str.contains("MONDO"))

    df = df.with_columns(
        [
            pl.col("mondo_id")
            .str.replace("MONDO_", "")
            .alias("node_id")
            .str.strip_chars_start("0")
        ]
    )

    df = df.sort(by=sorted(df.columns))
    return df  # type: ignore[no-any-return]


diseases_node = node(
    process_diseases,
    inputs={
        "diseases": "landing.opentargets.diseases",
        "mondo_efo_mappings_df": "opentargets.mondo_efo_mappings",
    },
    outputs="opentargets.diseases",
    name="diseases",
    tags=["bronze"],
)
