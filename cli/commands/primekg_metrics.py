import json
from pathlib import Path

import polars as pl


def get_primekg_metrics(in_path: Path, out_path: Path):
    df = pl.read_csv(
        in_path,
        schema={
            "relation": pl.Utf8,
            "display_relation": pl.Utf8,
            "x_index": pl.Utf8,
            "x_id": pl.Utf8,
            "x_type": pl.Utf8,
            "x_name": pl.Utf8,
            "x_source": pl.Utf8,
            "y_index": pl.Utf8,
            "y_id": pl.Utf8,
            "y_type": pl.Utf8,
            "y_name": pl.Utf8,
            "y_source": pl.Utf8,
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
