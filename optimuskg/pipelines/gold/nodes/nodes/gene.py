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
    disease_protein: pl.DataFrame,
    effect_protein: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                anatomy_protein.select(  # TODO: replace to protein_anatomy name for the relation
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
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
                exposure_protein.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                drug_protein.select(
                    pl.col("y_id").alias("id"),
                    pl.col("y_type").alias("type"),
                    pl.col("y_name").alias("name"),
                    pl.col("y_source").alias("source"),
                ),
                protein_biological_process.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                protein_cellular_component.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                protein_molecular_function.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                pathway_protein.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                disease_protein.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
                effect_protein.select(
                    pl.col("x_id").alias("id"),
                    pl.col("x_type").alias("type"),
                    pl.col("x_name").alias("name"),
                    pl.col("x_source").alias("source"),
                ),
            ],
            how="vertical",
        )
        .filter(pl.col("type") == "gene")
        .unique()
    )


gene_node = node(
    run,
    inputs={
        "anatomy_protein": "silver.anatomy_protein",
        "opentargets_edges": "silver.opentargets.opentargets_edges",
        "exposure_protein": "silver.exposure_protein",
        "drug_protein": "silver.drug_protein",
        "protein_biological_process": "silver.protein_biological_process",
        "protein_cellular_component": "silver.protein_cellular_component",
        "protein_molecular_function": "silver.protein_molecular_function",
        "pathway_protein": "silver.pathway_protein",
        "disease_protein": "silver.disease_protein",
        "effect_protein": "silver.effect_protein",
    },
    outputs="nodes.gene",
    name="gene",
    tags=["gold"],
)
