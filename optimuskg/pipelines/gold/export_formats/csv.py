import logging

import polars as pl

logger = logging.getLogger(__name__)


def csv_export(
    nodes_dict: dict[str, pl.DataFrame], edges_dict: dict[str, pl.DataFrame]
) -> tuple[dict[str, pl.DataFrame], dict[str, pl.DataFrame]]:
    meta = {
        **{
            f"nodes/{name}.csv": df.with_columns(
                pl.col("properties").struct.json_encode()
            )
            if "properties" in df.columns
            else df
            for name, df in nodes_dict.items()
        },
        **{
            f"edges/{name}.csv": df.with_columns(
                pl.col("properties").struct.json_encode()
            )
            if "properties" in df.columns
            else df
            for name, df in edges_dict.items()
        },
    }

    no_meta = {
        "nodes.csv": pl.concat([df.drop("properties") for _, df in nodes_dict.items()]),
        "edges.csv": pl.concat([df.drop("properties") for _, df in edges_dict.items()]),
        **{
            f"nodes/{name}.csv": df.drop("properties")
            for name, df in nodes_dict.items()
        },
        **{
            f"edges/{name}.csv": df.drop("properties")
            for name, df in edges_dict.items()
        },
    }
    return meta, no_meta
