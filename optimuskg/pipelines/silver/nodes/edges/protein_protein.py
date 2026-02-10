import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import (
    Edge,
    Node,
    Relation,
    Source,
    resolve_sources,
)


def run(
    protein_protein: pl.DataFrame,
    ensembl_ncbi_mapping: pl.DataFrame,
) -> pl.DataFrame:
    return (
        protein_protein.join(
            ensembl_ncbi_mapping, left_on="from", right_on="ncbi_id", how="left"
        )
        .join(ensembl_ncbi_mapping, left_on="to", right_on="ncbi_id", how="left")
        .unique(subset=["from", "to"])
        .select(
            pl.coalesce("ensembl_id", "from").alias("from"),
            pl.coalesce("ensembl_id_right", "to").alias("to"),
            pl.lit(Edge.format_label(Node.PROTEIN, Node.PROTEIN)).alias("label"),
            pl.lit(Relation.INTERACTS_WITH).alias("relation"),
            pl.lit(False).alias("undirected"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit([Source.PRIMEKG]).alias("direct"),
                            pl.col("databases")
                            .map_elements(
                                resolve_sources, return_dtype=pl.List(pl.Utf8)
                            )
                            .alias("indirect"),
                        ]
                    ).alias("sources"),
                ]
            ).alias("properties"),
        )
        .filter(pl.col("from").is_not_null() & pl.col("to").is_not_null())
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


protein_protein_node = node(
    run,
    inputs={
        "protein_protein": "bronze.ppi.protein_protein",
        "ensembl_ncbi_mapping": "bronze.opentargets.ensembl_ncbi_mapping",
    },
    outputs="edges.protein_protein",
    name="protein_protein",
    tags=["silver"],
)
