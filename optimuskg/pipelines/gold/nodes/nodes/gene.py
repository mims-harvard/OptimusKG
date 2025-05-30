import polars as pl
from kedro.pipeline import node


def process_gene_nodes(  # noqa: PLR0913
    gene_expressions_in_anatomy: pl.DataFrame,
    opentargets_edges: pl.DataFrame,
    ctd_exposure_protein_interactions: pl.DataFrame,
    drug_protein: pl.DataFrame,
    protein_biological_process_interactions: pl.DataFrame,
    protein_cellular_component_interactions: pl.DataFrame,
    protein_molecular_function_interactions: pl.DataFrame,
    pathway_protein_interactions: pl.DataFrame,
) -> pl.DataFrame:
    bgee_gene_nodes = (
        gene_expressions_in_anatomy.filter(pl.col("x_type") == "gene")
        .select(
            pl.col("x_id").alias("id"),
            pl.col("x_type").alias("type"),
            pl.col("x_name").alias("name"),
            pl.col("x_source").alias("source"),
        )
        .unique()
    )

    opentargets_gene_nodes_x = (
        opentargets_edges.filter(pl.col("x_type") == "gene")
        .select(
            pl.col("x_id").alias("id"),
            pl.col("x_type").alias("type"),
            pl.col("x_name").alias("name"),
            pl.col("x_source").alias("source"),
        )
        .unique()
    )
    opentargets_gene_nodes_y = (
        opentargets_edges.filter(pl.col("y_type") == "gene")
        .select(
            pl.col("y_id").alias("id"),
            pl.col("y_type").alias("type"),
            pl.col("y_name").alias("name"),
            pl.col("y_source").alias("source"),
        )
        .unique()
    )
    opentargets_gene_nodes = pl.concat(
        [opentargets_gene_nodes_x, opentargets_gene_nodes_y]
    ).unique()

    ctd_exposure_protein_interactions = (
        ctd_exposure_protein_interactions.filter(pl.col("y_type") == "gene")
        .select(
            pl.col("y_id").alias("id"),
            pl.col("y_type").alias("type"),
            pl.col("y_name").alias("name"),
            pl.col("y_source").alias("source"),
        )
        .unique()
    )
    drug_protein = (
        drug_protein.filter(pl.col("y_type") == "gene")
        .select(
            pl.col("y_id").alias("id"),
            pl.col("y_type").alias("type"),
            pl.col("y_name").alias("name"),
            pl.col("y_source").alias("source"),
        )
        .unique()
    )
    protein_biological_process_interactions = (
        protein_biological_process_interactions.filter(
            pl.col("x_type") == "gene"
        ).select(
            pl.col("x_id").alias("id"),
            pl.col("x_type").alias("type"),
            pl.col("x_name").alias("name"),
            pl.col("x_source").alias("source"),
        )
    ).unique()
    protein_cellular_component_interactions = (
        protein_cellular_component_interactions.filter(
            pl.col("x_type") == "gene"
        ).select(
            pl.col("x_id").alias("id"),
            pl.col("x_type").alias("type"),
            pl.col("x_name").alias("name"),
            pl.col("x_source").alias("source"),
        )
    ).unique()
    protein_molecular_function_interactions = (
        protein_molecular_function_interactions.filter(
            pl.col("x_type") == "gene"
        ).select(
            pl.col("x_id").alias("id"),
            pl.col("x_type").alias("type"),
            pl.col("x_name").alias("name"),
            pl.col("x_source").alias("source"),
        )
    ).unique()
    pathway_protein_interactions = (
        pathway_protein_interactions.filter(pl.col("x_type") == "gene")
        .select(
            pl.col("x_id").alias("id"),
            pl.col("x_type").alias("type"),
            pl.col("x_name").alias("name"),
            pl.col("x_source").alias("source"),
        )
        .unique()
    )

    return pl.concat(
        [
            bgee_gene_nodes,
            opentargets_gene_nodes,
            ctd_exposure_protein_interactions,
            drug_protein,
            protein_biological_process_interactions,
            protein_cellular_component_interactions,
            protein_molecular_function_interactions,
            pathway_protein_interactions,
        ]
    ).unique()


gene_node = node(
    process_gene_nodes,
    inputs={
        "gene_expressions_in_anatomy": "silver.bgee.gene_expressions_in_anatomy",
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "ctd_exposure_protein_interactions": "silver.ctd.ctd_exposure_protein_interactions",
        "drug_protein": "silver.drugbank.drug_protein",
        "protein_biological_process_interactions": "silver.ncbigene.protein_biological_process_interactions",
        "protein_cellular_component_interactions": "silver.ncbigene.protein_cellular_component_interactions",
        "protein_molecular_function_interactions": "silver.ncbigene.protein_molecular_function_interactions",
        "pathway_protein_interactions": "silver.reactome.pathway_protein_interactions",
    },
    outputs="nodes.gene",
    name="gene",
    tags=["gold"],
)
