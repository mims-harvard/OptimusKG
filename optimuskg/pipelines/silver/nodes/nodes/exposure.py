import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Node


def run(  # noqa: PLR0913
    ctd_exposure_events: pl.DataFrame,
    exposure_protein: pl.DataFrame,
    exposure_exposure: pl.DataFrame,
    exposure_disease: pl.DataFrame,
    exposure_biological_process: pl.DataFrame,
    exposure_molecular_function: pl.DataFrame,
    exposure_cellular_component: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                exposure_exposure.select(
                    pl.concat_list(["from", "to"]).explode().alias("id")
                ),
                exposure_biological_process.select(pl.col("from").alias("id")),
                exposure_cellular_component.select(pl.col("from").alias("id")),
                exposure_disease.select(pl.col("from").alias("id")),
                exposure_molecular_function.select(pl.col("from").alias("id")),
                exposure_protein.select(pl.col("from").alias("id")),
            ]
        )
        .unique(subset="id")
        .join(
            ctd_exposure_events.select(
                pl.col("exposure_stressor_id"),
                pl.col("exposure_stressor_name"),
                pl.col("stressor_source_category"),
                pl.col("stressor_source_details"),
            ).unique(subset="exposure_stressor_id"),
            left_on="id",
            right_on="exposure_stressor_id",
            how="inner",
        )
        .select(
            [
                pl.col("id"),
                pl.lit(Node.EXPOSURE).alias("label"),
                pl.struct(
                    [
                        pl.lit(["CTD"]).alias("direct_sources"),
                        pl.lit(["MESH"]).alias("indirect_sources"),
                        pl.col("exposure_stressor_name").alias("name"),
                        pl.col("stressor_source_category")
                        .str.split("|")
                        .alias("source_categories"),
                        pl.col("stressor_source_details").alias("source_details"),
                    ]
                ).alias("properties"),
            ]
        )
        .sort(by="id")
    )


exposure_node = node(
    run,
    inputs={
        "ctd_exposure_events": "bronze.ctd.ctd_exposure_events",
        "exposure_protein": "silver.edges.exposure_protein",
        "exposure_exposure": "silver.edges.exposure_exposure",
        "exposure_disease": "silver.edges.exposure_disease",
        "exposure_biological_process": "silver.edges.exposure_biological_process",
        "exposure_molecular_function": "silver.edges.exposure_molecular_function",
        "exposure_cellular_component": "silver.edges.exposure_cellular_component",
    },
    outputs="nodes.exposure",
    name="exposure",
    tags=["silver"],
)
