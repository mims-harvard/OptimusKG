import json
from pathlib import Path

import polars as pl


def get_primekg_metrics(in_path: Path, out_path: Path):
    df = pl.read_csv(
        in_path,
        schema={
            "relation": pl.String,
            "display_relation": pl.String,
            "x_index": pl.String,
            "x_id": pl.String,
            "x_type": pl.String,
            "x_name": pl.String,
            "x_source": pl.String,
            "y_index": pl.String,
            "y_id": pl.String,
            "y_type": pl.String,
            "y_name": pl.String,
            "y_source": pl.String,
        },
    )

    total_rows = len(df)

    relation_counts = df.get_column("relation").value_counts()

    metrics = {"total_rows": total_rows, "relations": []}

    for row in relation_counts.iter_rows(named=True):
        relation = row["relation"]
        count = row["count"]
        percentage = (count / total_rows) * 100
        metrics["relations"].append(
            {
                "relation": relation,
                "count": count,
                "percentage": f"{percentage:.2f}%",
            }
        )

    with out_path.open("w") as f:
        json.dump(metrics, f, indent=2)

    return metrics
