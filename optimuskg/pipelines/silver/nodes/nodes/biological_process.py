import polars as pl
from kedro.pipeline import node


def run(
    biological_process_protein: pl.DataFrame,
    exposure_biological_process: pl.DataFrame,
    biological_process_biological_process: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                biological_process_protein.select(pl.col("from").alias("id")),
                exposure_biological_process.select(pl.col("to").alias("id")),
                biological_process_biological_process.select(
                    pl.concat_list(["from", "to"]).explode().alias("id")
                ),
            ]
        )
        .unique(subset="id")
        .join(
            go_terms, left_on="id", right_on="id", how="left"
        )  # TODO: there are 3 ids that are not in the go_terms table
        .select(
            pl.col("id"),
            pl.lit("biological_process").alias("node_type"),
            pl.struct(
                [
                    pl.lit("GO").alias("source"),
                    pl.col("name"),
                    pl.col("definition"),
                    pl.col("xrefs"),  # TODO: cast to a proper list
                    pl.col("synonyms"),  # TODO: cast to a proper list
                    pl.col("ontology_description").alias("ontologyDescription"),
                    pl.col("ontology_title").alias("ontologyTitle"),
                    pl.col("ontology_license").alias("ontologyLicense"),
                    pl.col("ontology_version").alias("ontologyVersion"),
                ]
            ).alias("properties"),
        )
        .sort(by="id")
    )


biological_process_node = node(
    run,
    inputs={
        "biological_process_protein": "silver.edges.biological_process_protein",
        "exposure_biological_process": "silver.edges.exposure_biological_process",
        "biological_process_biological_process": "silver.edges.biological_process_biological_process",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="nodes.biological_process",
    name="biological_process",
    tags=["silver"],
)
