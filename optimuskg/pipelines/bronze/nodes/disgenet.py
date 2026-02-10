import polars as pl
from kedro.pipeline import node


def run(
    curated_gene_disease_associations: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    curated_gene_disease_associations = curated_gene_disease_associations.rename(
        {
            "geneid": "gene_id",
            "geneSymbol": "gene_symbol",
            "DSI": "dsi",
            "DPI": "dpi",
            "diseaseId": "disease_id",
            "diseaseName": "disease_name",
            "diseaseType": "disease_type",
            "diesaseClass": "disease_class",  # NOTE: fixing typo from "diesase" to "disease"
            "diesaseSemanticType": "disease_semantic_type",  # NOTE: fixing typo from "diesase" to "disease"
            "score": "score",
            "EI": "ei",
            "YearInitial": "year_initial",
            "YearFinal": "year_final",
            "NofPmids": "nof_pmids",
            "NofSnps": "nof_snps",
            "source": "source",
        }
    )

    # Strip whitespaces from all string columns
    string_columns = [
        col
        for col, dtype in zip(
            curated_gene_disease_associations.columns,
            curated_gene_disease_associations.dtypes,
        )
        if dtype == pl.String
    ]
    curated_gene_disease_associations = curated_gene_disease_associations.with_columns(
        [pl.col(col).str.strip_chars() for col in string_columns]
    )

    curated_gene_disease_associations = curated_gene_disease_associations.with_columns(
        [
            pl.col("gene_id")
            .cast(pl.String)
            .map_elements(
                lambda x: f"NCBIGene:{x}",
                return_dtype=pl.String,
            )
            .alias("gene_id"),
        ]
    )

    disgenet_phenotypes = curated_gene_disease_associations.filter(
        pl.col("disease_type") == "phenotype"
    ).sort(by=["gene_id", "disease_id"])
    diseases = curated_gene_disease_associations.filter(
        pl.col("disease_type") == "disease"
    ).sort(by=["gene_id", "disease_id"])
    return disgenet_phenotypes, diseases


disgenet_node = node(
    run,
    inputs={
        "curated_gene_disease_associations": "landing.disgenet.curated_gene_disease_associations",
    },
    outputs=["disgenet.disgenet_phenotypes", "disgenet.diseases"],
    name="disgenet",
    tags=["bronze"],
)
