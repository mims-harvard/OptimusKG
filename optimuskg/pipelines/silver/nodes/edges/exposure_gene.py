import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node, Relation, Source

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
                pl.len().alias("evidence_count"),
                pl.col("number_of_stressor_samples")
                .cast(pl.Int64)
                .sum()
                .alias("number_of_stressor_samples"),
                # TODO: we could add the stressor_notes column
                pl.col("number_of_receptors")
                .cast(pl.Int64)
                .sum()
                .alias("number_of_receptors"),
                pl.col("receptors").drop_nulls().unique().alias("receptors"),
                pl.col("receptor_notes").drop_nulls().unique().alias("receptor_notes"),
                pl.col("smoking_status")
                .str.split("|")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .alias("smoking_statuses"),
                pl.col("age_type")
                .filter(pl.col("age_type") != "null")
                .len()
                .alias("age_entries"),
                pl.when(pl.col("age_type") == "closed_range")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .alias("age_range_values"),
                pl.when(pl.col("age_type") == "mean")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .alias("age_mean_values"),
                pl.when(pl.col("age_type") == "median")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .alias("age_median_values"),
                pl.when(pl.col("age_type") == "point")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .alias("age_point_values"),
                pl.when(pl.col("age_type") == "open_range")
                .then(pl.col("age"))
                .drop_nulls()
                .unique()
                .alias("age_open_range_values"),
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
                pl.col("detection_limit")
                .drop_nulls()
                .unique()
                .alias("detection_limit"),
                pl.col("detection_limit_uom")
                .drop_nulls()
                .unique()
                .alias("detection_limit_uom"),
                pl.col("detection_frequency")
                .drop_nulls()
                .unique()
                .alias("detection_frequency"),
                pl.col("medium").drop_nulls().unique().alias("mediums"),
                pl.col("assay_notes").drop_nulls().unique().alias("assay_notes"),
                # TODO: we can add this columns with a similar approach of the ages columns: marker_level, marker_units_of_measurement, marker_measurement_statistic
                pl.col("study_countries")
                .str.split("|")
                .explode()
                .drop_nulls()
                .unique()
                .alias("study_countries"),
                pl.col("state_or_province")
                .str.split("|")
                .explode()
                .drop_nulls()
                .unique()
                .alias("states_or_provinces"),
                pl.col("city_town_region_area")
                .str.split("|")
                .explode()
                .drop_nulls()
                .unique()
                .alias("city_town_region_areas"),
                pl.col("exposure_event_notes")
                .drop_nulls()
                .unique()
                .alias("exposure_event_notes"),
                pl.col("outcome_relationship")
                .drop_nulls()
                .unique()
                .alias("outcome_relationships"),
                pl.col("exposure_outcome_notes")
                .drop_nulls()
                .unique()
                .alias("exposure_outcome_notes"),
                pl.col("reference").drop_nulls().unique().alias("references"),
                pl.col("associated_study_titles")
                .drop_nulls()
                .unique()
                .alias("associated_study_titles"),
                pl.col("enrollment_start_year")
                .drop_nulls()
                .unique()
                .alias("enrollment_start_years"),
                pl.col("enrollment_end_year")
                .drop_nulls()
                .unique()
                .alias("enrollment_end_years"),
                pl.col("study_factors")
                .str.split("|")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .alias("study_factors"),
            ]
        )
        .select(
            [
                pl.col("from"),
                pl.col("to"),
                pl.lit(Edge.format_label(Node.EXPOSURE, Node.GENE)).alias("label"),
                pl.lit(Relation.INTERACTS_WITH).alias(
                    "relation"
                ),  # NOTE: this is the same relation type used in PrimeKG, but we need to validate if this is consistent with the ontology tree.
                pl.lit(False).alias("undirected"),
                pl.struct(
                    [
                        pl.struct(
                            [
                                pl.lit([Source.CTD, Source.OPEN_TARGETS])
                                .cast(pl.List(pl.String))
                                .alias("direct"),
                                pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                            ]
                        ).alias("sources"),
                        pl.col("evidence_count"),
                        pl.col("number_of_receptors"),
                        pl.col("receptors"),
                        pl.col("receptor_notes"),
                        pl.col("smoking_statuses"),
                        pl.col("age_entries"),
                        pl.col("age_range_values"),
                        pl.col("age_mean_values"),
                        pl.col("age_median_values"),
                        pl.col("age_point_values"),
                        pl.col("age_open_range_values"),
                        pl.col("sexes"),
                        pl.col("races"),
                        pl.col("methods"),
                        pl.col("detection_limit"),
                        pl.col("detection_limit_uom"),
                        pl.col("detection_frequency"),
                        pl.col("mediums"),
                        pl.col("assay_notes"),
                        pl.col("study_countries"),
                        pl.col("states_or_provinces"),
                        pl.col("city_town_region_areas"),
                        pl.col("exposure_event_notes"),
                        pl.col("outcome_relationships"),
                        pl.col("exposure_outcome_notes"),
                        pl.col("references"),
                        pl.col("associated_study_titles"),
                        pl.col("enrollment_start_years"),
                        pl.col("enrollment_end_years"),
                        pl.col("study_factors"),
                    ]
                ).alias("properties"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


exposure_gene_node = node(
    run,
    inputs={
        "ctd_exposure_events": "bronze.ctd.ctd_exposure_events",
        "target": "bronze.opentargets.target",
    },
    outputs="edges.exposure_gene",
    name="exposure_gene",
    tags=["silver"],
)
