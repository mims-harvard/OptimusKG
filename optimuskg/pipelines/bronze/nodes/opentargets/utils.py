from collections.abc import Callable
from typing import Any

import pandas as pd
import polars as pl


def concat_json_partitions(
    partitioned_input: dict[str, Callable[[], Any]],
) -> pl.DataFrame:
    """
    Concatenate JSON partitions while enforcing a consistent schema.

    Args:
        partitioned_input: Dictionary mapping partition keys to loader functions
        target_schema: Target schema to enforce on all partitions

    Returns:
        Concatenated DataFrame with enforced schema
    """
    partitions = []

    for _, partition_load_func in sorted(partitioned_input.items()):
        partition_data = partition_load_func()
        partitions.append(partition_data)

    concated_df = pd.concat(partitions)
    concated_df = pl.from_pandas(concated_df)
    assert isinstance(concated_df, pl.DataFrame)
    return concated_df

def concat_partitions(
    partitioned_input: dict[str, Callable[[], Any]],
) -> pl.DataFrame:
    """Concatenate input partitions into one Polars DataFrame.

    Args:
        partitioned_input: A dictionary with partition ids as keys and load functions as values.

    Returns:
        Polars DataFrame representing a concatenation of all loaded partitions.
    """
    partitions = []

    for _, partition_load_func in sorted(partitioned_input.items()):
        partition_data = partition_load_func()
        pl_data = pl.from_pandas(partition_data)

        # Standardize sex column to pl.List(pl.String)
        if "sex" in pl_data.columns:
            pl_data = pl_data.with_columns(
                pl.col("sex").map_elements(
                    lambda x: [x] if isinstance(x, str) else x,
                    return_dtype=pl.List(pl.String),
                )
            )
        partitions.append(pl_data)

    concated_df = pl.concat(partitions, how="vertical")
    assert isinstance(concated_df, pl.DataFrame)
    return concated_df