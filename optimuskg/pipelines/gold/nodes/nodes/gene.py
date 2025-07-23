import polars as pl
from kedro.pipeline import node


def run(  # noqa: PLR0913
    anatomy_protein: pl.DataFrame,
    opentargets_edges: pl.DataFrame,
    exposure_protein: pl.DataFrame,
    drug_protein: pl.DataFrame,
    protein_biological_process: pl.DataFrame,
    protein_cellular_component: pl.DataFrame,
    protein_molecular_function: pl.DataFrame,
    pathway_protein: pl.DataFrame,
    disgenet_disease_protein: pl.DataFrame,
    disgenet_effect_protein: pl.DataFrame,
) -> pl.DataFrame:
    bgee_nodes = anatomy_protein.select(
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

    ep_nodes = exposure_protein.select(
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

    pbp_nodes = protein_biological_process.select(
        pl.col("x_id").alias("id"),
        pl.col("x_type").alias("type"),
        pl.col("x_name").alias("name"),
        pl.col("x_source").alias("source"),
    )

    pcc_nodes = protein_cellular_component.select(
        pl.col("x_id").alias("id"),
        pl.col("x_type").alias("type"),
        pl.col("x_name").alias("name"),
        pl.col("x_source").alias("source"),
    )

    pmf_nodes = protein_molecular_function.select(
        pl.col("x_id").alias("id"),
        pl.col("x_type").alias("type"),
        pl.col("x_name").alias("name"),
        pl.col("x_source").alias("source"),
    )

    pp_nodes = pathway_protein.select(
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
    run,
    inputs={
        "anatomy_protein": "silver.bgee.anatomy_protein",
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "exposure_protein": "silver.ctd.exposure_protein",
        "drug_protein": "silver.drugbank.drug_protein",
        "protein_biological_process": "silver.ncbigene.protein_biological_process",
        "protein_cellular_component": "silver.ncbigene.protein_cellular_component",
        "protein_molecular_function": "silver.ncbigene.protein_molecular_function",
        "pathway_protein": "silver.reactome.pathway_protein",
        "disgenet_disease_protein": "silver.disgenet.disease_protein",
        "disgenet_effect_protein": "silver.disgenet.effect_protein",
    },
    outputs="nodes.gene",
    name="gene",
    tags=["gold"],
)
