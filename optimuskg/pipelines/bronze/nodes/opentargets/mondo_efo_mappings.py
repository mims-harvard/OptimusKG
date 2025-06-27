import polars as pl
from kedro.pipeline import node


def run(
    mondo_efo_mappings_df: pl.DataFrame,
) -> pl.DataFrame:
    df = mondo_efo_mappings_df.with_columns(
        [
            pl.col("mondo_id").str.replace("http://purl.obolibrary.org/obo/", ""),
            pl.col("efo_id")
            .str.replace("http://www.ebi.ac.uk/efo/", "")
            .str.replace("http://www.orpha.net/ORDO/", "")
            .str.replace("http://purl.obolibrary.org/obo/", ""),
        ]
    )

    df = df.select(["mondo_id", "efo_id"])
    df = df.sort("mondo_id")
    return df


mondo_efo_mappings_node = node(
    run,
    inputs={"mondo_efo_mappings_df": "landing.opentargets.mondo_efo_mappings"},
    outputs="opentargets.mondo_efo_mappings",
    name="mondo_efo_mappings",
    tags=["bronze"],
)
