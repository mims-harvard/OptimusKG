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
    disgenet_disease_protein: pl.DataFrame,
    disgenet_effect_protein: pl.DataFrame,
) -> pl.DataFrame:
    bgee_nodes = gene_expressions_in_anatomy.select(
        pl.col("x_id").alias("id"),
        pl.col("x_type").alias("type"),
        pl.col("x_name").alias("name"),
        pl.col("x_source").alias("source"),
    )

    ot_nodes = pl.concat(
        [
            opentargets_edges.select(
                pl.col("x_id").alias("id"),
                pl.col("x_type").alias("type"),
                pl.col("x_name").alias("name"),
                pl.col("x_source").alias("source"),
            ),
            opentargets_edges.select(
                pl.col("y_id").alias("id"),
                pl.col("y_type").alias("type"),
                pl.col("y_name").alias("name"),
                pl.col("y_source").alias("source"),
            ),
        ],
        how="vertical",
    )

    ep_nodes = ctd_exposure_protein_interactions.select(
        pl.col("y_id").alias("id"),
        pl.col("y_type").alias("type"),
        pl.col("y_name").alias("name"),
        pl.col("y_source").alias("source"),
    )

    dp_nodes = drug_protein.select(
        pl.col("y_id").alias("id"),
        pl.col("y_type").alias("type"),
        pl.col("y_name").alias("name"),
        pl.col("y_source").alias("source"),
    )

    pbp_nodes = protein_biological_process_interactions.select(
        pl.col("x_id").alias("id"),
        pl.col("x_type").alias("type"),
        pl.col("x_name").alias("name"),
        pl.col("x_source").alias("source"),
    )

    pcc_nodes = protein_cellular_component_interactions.select(
        pl.col("x_id").alias("id"),
        pl.col("x_type").alias("type"),
        pl.col("x_name").alias("name"),
        pl.col("x_source").alias("source"),
    )

    pmf_nodes = protein_molecular_function_interactions.select(
        pl.col("x_id").alias("id"),
        pl.col("x_type").alias("type"),
        pl.col("x_name").alias("name"),
        pl.col("x_source").alias("source"),
    )

    pp_nodes = pathway_protein_interactions.select(
        pl.col("x_id").alias("id"),
        pl.col("x_type").alias("type"),
        pl.col("x_name").alias("name"),
        pl.col("x_source").alias("source"),
    )

    ddp_nodes = disgenet_disease_protein.select(
        pl.col("x_id").alias("id"),
        pl.col("x_type").alias("type"),
        pl.col("x_name").alias("name"),
        pl.col("x_source").alias("source"),
    )

    dep_nodes = disgenet_effect_protein.select(
        pl.col("x_id").alias("id"),
        pl.col("x_type").alias("type"),
        pl.col("x_name").alias("name"),
        pl.col("x_source").alias("source"),
    )

    all_nodes = pl.concat(
        [
            bgee_nodes,
            ot_nodes,
            ep_nodes,
            dp_nodes,
            pbp_nodes,
            pcc_nodes,
            pmf_nodes,
            pp_nodes,
            ddp_nodes,
            dep_nodes,
        ],
        how="vertical",
    )

    return all_nodes.filter(pl.col("type") == "gene").unique()


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
        "disgenet_disease_protein": "silver.disgenet.disease_protein",
        "disgenet_effect_protein": "silver.disgenet.effect_protein",
    },
    outputs="nodes.gene",
    name="gene",
    tags=["gold"],
)
