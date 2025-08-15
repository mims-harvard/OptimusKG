import json
import logging
from pathlib import Path

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def transform_properties_to_lists(df: pl.DataFrame) -> pl.LazyFrame:
    """
    Transform properties struct fields to ensure all values are lists.
    This function unnests and rebuilds the struct to handle potential
    internal length inconsistencies in the source data.

    Args:
        df: Input DataFrame with 'properties' struct column

    Returns:
        DataFrame with transformed properties
    """

    lf = df.lazy()

    # Use collect_schema() to avoid a PerformanceWarning
    properties_schema = lf.collect_schema()["properties"]

    if not isinstance(properties_schema, pl.Struct):
        raise ValueError("'properties' column must be a Struct type")

    # Get the names of the fields inside the struct
    property_field_names = [field.name for field in properties_schema.fields]

    # Unnest the properties struct to work with its fields as top-level columns
    df_unnested = lf.unnest("properties")

    field_expressions = []
    for field in properties_schema.fields:
        field_name = field.name
        field_type = field.dtype

        if isinstance(field_type, pl.List):
            # Keep as-is
            field_expressions.append(pl.col(field_name))
        else:
            # Convert scalar to single-element list
            field_expressions.append(
                pl.when(pl.col(field_name).is_not_null())
                .then(pl.concat_list([pl.col(field_name)]))
                .otherwise(None)  # Handle null values
                .alias(field_name)
            )

    # Rebuild the 'properties' struct from the transformed columns
    # and drop the temporary unnested columns to restore the original schema shape
    return df_unnested.with_columns(
        pl.struct(field_expressions).alias("properties")
    ).drop(property_field_names)


def run(  # noqa: PLR0913
    # Nodes
    gene: pl.DataFrame,
    anatomy: pl.DataFrame,
    exposure: pl.DataFrame,
    drug: pl.DataFrame,
    disease: pl.DataFrame,
    phenotype: pl.DataFrame,
    biological_process: pl.DataFrame,
    cellular_component: pl.DataFrame,
    molecular_function: pl.DataFrame,
    pathway: pl.DataFrame,
    # Edges
    anatomy_protein: pl.DataFrame,
    biological_process_protein: pl.DataFrame,
    cellular_component_protein: pl.DataFrame,
    disease_protein: pl.DataFrame,
    disease_disease: pl.DataFrame,
    disease_phenotype: pl.DataFrame,
    drug_drug: pl.DataFrame,
    drug_protein: pl.DataFrame,
    drug_disease: pl.DataFrame,
    exposure_exposure: pl.DataFrame,
    exposure_protein: pl.DataFrame,
    exposure_disease: pl.DataFrame,
    molecular_function_protein: pl.DataFrame,
    pathway_pathway: pl.DataFrame,
    pathway_protein: pl.DataFrame,
    phenotype_protein: pl.DataFrame,
    exposure_biological_process: pl.DataFrame,
    exposure_molecular_function: pl.DataFrame,
    exposure_cellular_component: pl.DataFrame,
    cellular_component_cellular_component: pl.DataFrame,
    biological_process_biological_process: pl.DataFrame,
    molecular_function_molecular_function: pl.DataFrame,
    phenotype_phenotype: pl.DataFrame,
    anatomy_anatomy: pl.DataFrame,
    drug_phenotype: pl.DataFrame,
) -> pl.DataFrame:
    nodes = [
        gene,
        anatomy,
        exposure,
        drug,
        disease,
        phenotype,
        biological_process,
        cellular_component,
        molecular_function,
        pathway,
    ]

    edges = [
        anatomy_protein,
        biological_process_protein,
        cellular_component_protein,
        disease_protein,
        disease_disease,
        disease_phenotype,
        drug_drug,
        drug_protein,
        drug_disease,
        exposure_exposure,
        exposure_protein,
        exposure_disease,
        molecular_function_protein,
        pathway_pathway,
        pathway_protein,
        phenotype_protein,
        exposure_biological_process,
        exposure_molecular_function,
        exposure_cellular_component,
        cellular_component_cellular_component,
        biological_process_biological_process,
        molecular_function_molecular_function,
        phenotype_phenotype,
        anatomy_anatomy,
        drug_phenotype,
    ]

    output_path = Path("data/export/optimuskg.pg.jsonl")  # TODO: parameterize filepath
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_files = []

    try:
        # Process nodes
        for i, df in enumerate(nodes):
            logger.info(f"Processing node {i + 1}/{len(nodes)}...")

            temp_file = output_path.parent / f".temp_node_{i}.jsonl"
            temp_files.append(temp_file)

            (
                transform_properties_to_lists(df)
                .select(
                    [
                        pl.lit("node").alias("type"),
                        pl.col("id"),
                        pl.concat_list([pl.col("node_type")]).alias("labels"),
                        pl.col("properties"),
                    ]
                )
                .collect()
                .write_ndjson(temp_file)
            )

        # Process edges
        for i, df in enumerate(edges):
            logger.info(f"Processing edge {i + 1}/{len(edges)}...")

            temp_file = output_path.parent / f".temp_edge_{i}.jsonl"
            temp_files.append(temp_file)

            (
                transform_properties_to_lists(df)
                .select(
                    [
                        pl.lit("edge").alias("type"),
                        pl.col("from"),
                        pl.col("to"),
                        pl.concat_list([pl.col("relation")]).alias("labels"),
                        pl.col("undirected"),
                        pl.col("properties"),
                    ]
                )
                .collect()
                .write_ndjson(temp_file)
            )

        logger.info("Combining temporary files...")
        with open(output_path, "w") as outfile:
            for temp_file in temp_files:
                # Process file in chunks for memory efficiency
                with open(temp_file) as infile:
                    while True:
                        lines = infile.readlines(1024 * 1024)  # Read 1MB at a time
                        if not lines:
                            break

                        processed_lines = []
                        for line in lines:
                            data = json.loads(line)
                            if "properties" in data:
                                # Remove null properties with dict comprehension
                                data["properties"] = {
                                    k: v
                                    for k, v in data["properties"].items()
                                    if v is not None
                                }
                            processed_lines.append(json.dumps(data))

                        outfile.write("\n".join(processed_lines) + "\n")

        logger.info(f"Successfully wrote data to {output_path}")

    finally:
        logger.info("Cleaning up temporary files...")
        for temp_file in temp_files:
            if temp_file.exists():
                temp_file.unlink()


pg_export_node = node(
    run,
    inputs={
        # Nodes
        "gene": "silver.nodes.gene",
        "anatomy": "silver.nodes.anatomy",
        "exposure": "silver.nodes.exposure",
        "drug": "silver.nodes.drug",
        "disease": "silver.nodes.disease",
        "phenotype": "silver.nodes.phenotype",
        "biological_process": "silver.nodes.biological_process",
        "cellular_component": "silver.nodes.cellular_component",
        "molecular_function": "silver.nodes.molecular_function",
        "pathway": "silver.nodes.pathway",
        # Edges
        "anatomy_protein": "silver.edges.anatomy_protein",
        "biological_process_protein": "silver.edges.biological_process_protein",
        "cellular_component_protein": "silver.edges.cellular_component_protein",
        "disease_protein": "silver.edges.disease_protein",
        "disease_disease": "silver.edges.disease_disease",
        "disease_phenotype": "silver.edges.disease_phenotype",
        "drug_drug": "silver.edges.drug_drug",
        "drug_protein": "silver.edges.drug_protein",
        "drug_disease": "silver.edges.drug_disease",
        "exposure_exposure": "silver.edges.exposure_exposure",
        "exposure_protein": "silver.edges.exposure_protein",
        "exposure_disease": "silver.edges.exposure_disease",
        "molecular_function_protein": "silver.edges.molecular_function_protein",
        "pathway_pathway": "silver.edges.pathway_pathway",
        "pathway_protein": "silver.edges.pathway_protein",
        "phenotype_protein": "silver.edges.phenotype_protein",
        "exposure_biological_process": "silver.edges.exposure_biological_process",
        "exposure_molecular_function": "silver.edges.exposure_molecular_function",
        "exposure_cellular_component": "silver.edges.exposure_cellular_component",
        "cellular_component_cellular_component": "silver.edges.cellular_component_cellular_component",
        "biological_process_biological_process": "silver.edges.biological_process_biological_process",
        "molecular_function_molecular_function": "silver.edges.molecular_function_molecular_function",
        "phenotype_phenotype": "silver.edges.phenotype_phenotype",
        "anatomy_anatomy": "silver.edges.anatomy_anatomy",
        "drug_phenotype": "silver.edges.drug_phenotype",
    },
    outputs=None,
    tags=["gold"],
    name="pg_export",
)
