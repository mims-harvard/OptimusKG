import polars as pl
from kedro.pipeline import node


def run(
    gene_names: pl.DataFrame,
) -> pl.DataFrame:
    return (
        gene_names.rename(
            {"NCBI Gene ID(supplied by NCBI)": "ncbi_id", "Approved symbol": "symbol"}
        )
        .with_columns(
            pl.col("ncbi_id")
            .cast(pl.String)
            .map_elements(
                lambda x: f"NCBIGene:{x}",
                return_dtype=pl.String,
            )
            .alias("ncbi_id")
        )
        .select(["ncbi_id", "symbol"])
        .drop_nulls()
        .unique()  # Remove any duplicate mappings
        .sort("ncbi_id")
    )


gene_names_node = node(
    run,
    inputs={"gene_names": "landing.gene_names.gene_names"},
    outputs="gene_names.gene_names",
    name="gene_names",
    tags=["bronze"],
)
