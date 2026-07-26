import polars as pl
from kedro.pipeline import node


def run(
    combined_default_networks: pl.DataFrame,
    identifier_mappings: pl.DataFrame,
    target: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    ensembl_candidates = identifier_mappings.filter(
        pl.col("Source") == "Ensembl Gene ID"
    ).select(
        pl.col("Preferred_Name").alias("genemania_id"),
        pl.col("Name").alias("ensembl_id"),
    )

    direct_ensembl_mapping = ensembl_candidates.filter(
        (pl.col("genemania_id") == pl.col("ensembl_id"))
        & pl.col("genemania_id").str.contains(r"^ENSG\d+$")
    )

    remapped_ensembl_mapping = (
        ensembl_candidates.join(
            direct_ensembl_mapping.select("genemania_id"),
            on="genemania_id",
            how="anti",
        )
        .join(
            target.select(pl.col("id").alias("ensembl_id")),
            on="ensembl_id",
            how="inner",
        )
        .sort(by=["genemania_id", "ensembl_id"])
        .unique(subset=["genemania_id"], keep="first", maintain_order=True)
    )

    ensembl_mapping = (
        pl.concat([direct_ensembl_mapping, remapped_ensembl_mapping])
        .unique(subset=["genemania_id"])
        .sort(by=["genemania_id"])
    )

    gene_gene = (
        combined_default_networks.select(
            pl.col("Gene_A").alias("from"),
            pl.col("Gene_B").alias("to"),
            pl.col("Weight").cast(pl.Float64).alias("weight"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )

    return gene_gene, ensembl_mapping


genemania_node = node(
    run,
    inputs={
        "combined_default_networks": "landing.genemania.combined_default_networks",
        "identifier_mappings": "landing.genemania.identifier_mappings",
        "target": "bronze.opentargets.target",
    },
    outputs=[
        "genemania.gene_gene",
        "genemania.ensembl_mapping",
    ],
    name="genemania",
    tags=["bronze"],
)
