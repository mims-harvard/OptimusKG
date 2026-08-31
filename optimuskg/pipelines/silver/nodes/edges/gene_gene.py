import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import (
    Edge,
    GeneInteractionType,
    Node,
    Relation,
    Source,
    resolve_sources,
)


def run(
    gene_gene: pl.DataFrame,
    ensembl_ncbi_mapping: pl.DataFrame,
    genemania_gene_gene: pl.DataFrame,
    genemania_ensembl_mapping: pl.DataFrame,
) -> pl.DataFrame:
    # NOTE: Both sources store one row per unordered pair but with no consistent orientation, so we first order lexicographically so the smaller of the `from`, `to`, is on the left for easier processing.
    ppi_gene_gene = (
        gene_gene.join(
            ensembl_ncbi_mapping, left_on="from", right_on="ncbi_id", how="left"
        )
        .join(ensembl_ncbi_mapping, left_on="to", right_on="ncbi_id", how="left")
        .unique(subset=["from", "to"])
        .select(
            pl.coalesce("ensembl_id", "from").alias("from"),
            pl.coalesce("ensembl_id_right", "to").alias("to"),
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
            ).alias("ppi_properties"),
        )
        .filter(pl.col("from").is_not_null() & pl.col("to").is_not_null())
        .select(
            pl.min_horizontal("from", "to").alias("from"),
            pl.max_horizontal("from", "to").alias("to"),
            pl.col("ppi_properties"),
        )
        .unique(subset=["from", "to"])
    )

    assert (
        genemania_ensembl_mapping["ensembl_id"].n_unique()
        == genemania_ensembl_mapping.height
    ), (
        "GeneMANIA accession to Ensembl gene mapping is not injective: "
        f"{genemania_ensembl_mapping.height} accessions resolve to "
        f"{genemania_ensembl_mapping['ensembl_id'].n_unique()} distinct genes."
    )

    genemania = (
        genemania_gene_gene.join(
            genemania_ensembl_mapping,
            left_on="from",
            right_on="genemania_id",
            how="inner",
        )
        .join(
            genemania_ensembl_mapping,
            left_on="to",
            right_on="genemania_id",
            how="inner",
        )
        .select(
            pl.min_horizontal("ensembl_id", "ensembl_id_right").alias("from"),
            pl.max_horizontal("ensembl_id", "ensembl_id_right").alias("to"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit([Source.GENEMANIA])
                            .cast(pl.List(pl.String))
                            .alias("direct"),
                            pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                        ]
                    ).alias("sources"),
                    pl.col("weight"),
                ]
            ).alias("genemania_properties"),
        )
        .unique(subset=["from", "to"])
    )

    return (
        ppi_gene_gene.join(genemania, on=["from", "to"], how="full", coalesce=True)
        .select(
            pl.col("from"),
            pl.col("to"),
            pl.lit(Edge.format_label(Node.GENE, Node.GENE)).alias("label"),
            pl.lit(Relation.INTERACTS_WITH).alias("relation"),
            pl.lit(True).alias("undirected"),
            pl.struct(
                [
                    pl.when(pl.col("ppi_properties").is_not_null())
                    .then(
                        pl.lit(GeneInteractionType.PHYSICAL_PROTEIN_PROTEIN_INTERACTION)
                    )
                    .otherwise(pl.lit(GeneInteractionType.FUNCTIONAL))
                    .alias("interaction_type"),
                    pl.col("genemania_properties").struct.field("weight"),
                    pl.struct(
                        [
                            pl.concat_list(
                                [
                                    pl.col("ppi_properties")
                                    .struct.field("sources")
                                    .struct.field("direct")
                                    .fill_null([]),
                                    pl.col("genemania_properties")
                                    .struct.field("sources")
                                    .struct.field("direct")
                                    .fill_null([]),
                                ]
                            )
                            .list.unique()
                            .alias("direct"),
                            pl.col("ppi_properties")
                            .struct.field("sources")
                            .struct.field("indirect")
                            .fill_null([])
                            .alias("indirect"),
                        ]
                    ).alias("sources"),
                ]
            ).alias("properties"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


gene_gene_node = node(
    run,
    inputs={
        "gene_gene": "bronze.ppi.gene_gene",
        "ensembl_ncbi_mapping": "bronze.opentargets.ensembl_ncbi_mapping",
        "genemania_gene_gene": "bronze.genemania.gene_gene",
        "genemania_ensembl_mapping": "bronze.genemania.ensembl_mapping",
    },
    outputs="edges.gene_gene",
    name="gene_gene",
    tags=["silver"],
)
