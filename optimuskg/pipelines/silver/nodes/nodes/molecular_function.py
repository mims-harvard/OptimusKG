import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Node


def run(
    molecular_function_protein: pl.DataFrame,
    exposure_molecular_function: pl.DataFrame,
    molecular_function_molecular_function: pl.DataFrame,
    go_terms: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                molecular_function_protein.select(pl.col("from").alias("id")),
                exposure_molecular_function.select(pl.col("to").alias("id")),
                molecular_function_molecular_function.select(
                    pl.concat_list(["from", "to"]).explode().alias("id")
                ),
            ]
        )
        .unique(subset="id")
        .join(
            go_terms, left_on="id", right_on="id", how="left"
        )  # TODO: there are 2 ids that are not in the go_terms table
        .select(
            pl.col("id"),
            pl.lit(Node.MOLECULAR_FUNCTION).alias("label"),
            pl.struct(
                [
                    pl.lit("GO").alias("source"),
                    pl.col("name"),
                    pl.col("definition"),
                    pl.col("xrefs"),
                    pl.col("synonyms"),
                    pl.col("ontology"),
                ]
            ).alias("properties"),
        )
        .sort(by="id")
    )


molecular_function_node = node(
    run,
    inputs={
        "molecular_function_protein": "silver.edges.molecular_function_protein",
        "exposure_molecular_function": "silver.edges.exposure_molecular_function",
        "molecular_function_molecular_function": "silver.edges.molecular_function_molecular_function",
        "go_terms": "bronze.ontology.go_terms",
    },
    outputs="nodes.molecular_function",
    name="molecular_function",
    tags=["silver"],
)
