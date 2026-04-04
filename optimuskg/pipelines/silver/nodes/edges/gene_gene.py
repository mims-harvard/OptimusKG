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
    gene_gene: pl.DataFrame,
    ensembl_ncbi_mapping: pl.DataFrame,
) -> pl.DataFrame:
    return (
        gene_gene.join(
            ensembl_ncbi_mapping, left_on="from", right_on="ncbi_id", how="left"
        )
        .join(ensembl_ncbi_mapping, left_on="to", right_on="ncbi_id", how="left")
        .unique(subset=["from", "to"])
        .select(
            pl.coalesce("ensembl_id", "from").alias("from"),
            pl.coalesce("ensembl_id_right", "to").alias("to"),
            pl.lit(Edge.format_label(Node.GENE, Node.GENE)).alias("label"),
            pl.lit(Relation.INTERACTS_WITH).alias("relation"),
            pl.lit(False).alias("undirected"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit([Source.PRIMEKG])
                            .cast(pl.List(pl.String))
                            .alias("direct"),
                            pl.col("databases")
                            .map_elements(
                                resolve_sources, return_dtype=pl.List(pl.String)
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


gene_gene_node = node(
    run,
    inputs={
        "gene_gene": "bronze.ppi.gene_gene",
        "ensembl_ncbi_mapping": "bronze.opentargets.ensembl_ncbi_mapping",
    },
    outputs="edges.gene_gene",
    name="gene_gene",
    tags=["silver"],
)
