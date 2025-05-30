import polars as pl
from kedro.pipeline import node


def process_reactome_ncbi(
    ncbi2_reactome_df: pl.DataFrame,
) -> pl.DataFrame:
    df = (
        ncbi2_reactome_df.filter(pl.col("species") == "Homo sapiens")
        .drop("species")
        .unique()
        .with_columns(
            pl.col("ncbi_id")
            .cast(pl.Utf8)
            .map_elements(
                lambda x: f"NCBIGene:{x}",
                return_dtype=pl.Utf8,
            )
            .alias("ncbi_id"),
            pl.col("reactome_id")
            .cast(pl.Utf8)
            .map_elements(
                lambda x: f"REACT:{x}",  # NOTE: Biolink prefix for Reactome identifiers https://github.com/biolink/biolink-model/blob/56bb3a024d8c88c0ce75267cc7d0b8a1baf7f88e/project/prefixmap/biolink_model_prefix_map.json#L160C14-L160C55
                return_dtype=pl.Utf8,
            )
            .alias("reactome_id"),
        )
    )

    df = df.sort(by=sorted(df.columns))
    return df


reactome_ncbi_node = node(
    process_reactome_ncbi,
    inputs={"ncbi2_reactome_df": "landing.reactome.ncbi2_reactome"},
    outputs="reactome.reactome_ncbi",
    name="reactome_ncbi",
    tags=["bronze"],
)
