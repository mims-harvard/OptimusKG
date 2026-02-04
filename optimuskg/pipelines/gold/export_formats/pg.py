import json
import logging
from pathlib import Path

import polars as pl

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


def pg_export(nodes_dict: dict[str, pl.DataFrame], edges_dict: dict[str, pl.DataFrame]):
    output_path = Path("data/gold/optimuskg.pg.jsonl")  # TODO: parameterize filepath
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_files = []

    try:  # TODO: parameterize meta/no-meta for the export
        # Process nodes
        for i, df in enumerate(nodes_dict.values()):
            logger.info(f"Processing node {i + 1}/{len(nodes_dict.values())}...")

            temp_file = output_path.parent / f".temp_node_{i}.jsonl"
            temp_files.append(temp_file)

            (
                transform_properties_to_lists(df)
                .select(
                    [
                        pl.lit("node").alias("type"),
                        pl.col("id"),
                        pl.concat_list([pl.col("label")]).alias("labels"),
                        pl.col("properties"),
                    ]
                )
                .collect()
                .write_ndjson(temp_file)
            )

        # Process edges
        for i, df in enumerate(edges_dict.values()):
            logger.info(f"Processing edge {i + 1}/{len(edges_dict.values())}...")

            temp_file = output_path.parent / f".temp_edge_{i}.jsonl"
            temp_files.append(temp_file)

            (
                transform_properties_to_lists(df)
                .select(
                    [
                        pl.lit("edge").alias("type"),
                        pl.col("from"),
                        pl.col("to"),
                        pl.concat_list([pl.col("label")]).alias("labels"),
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
