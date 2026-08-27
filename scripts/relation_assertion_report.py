"""Quantify the effect of provenance-preserving relation resolution.

Compares, on the real silver edge tables, the collapsed ``relation`` column
against the full set of source-specific assertions now retained on each edge.

Usage:
    uv run python scripts/relation_assertion_report.py
"""

import polars as pl

EDGES = [
    "drug_disease",
    "drug_drug",
    "drug_gene",
    "disease_phenotype",
]


def report(name: str) -> dict[str, object]:
    """Summarize assertion retention and conflicts for one edge table."""
    df = pl.read_parquet(f"data/silver/edges/{name}.parquet")

    df = df.with_columns(
        pl.col("properties").struct.field("relation_assertions").alias("assertions"),
        pl.col("properties").struct.field("relation_conflict").alias("conflict"),
    ).with_columns(
        pl.col("assertions")
        .list.eval(pl.element().struct.field("relation"))
        .list.unique()
        .alias("distinct_relations")
    )

    total = df.height
    multi = df.filter(pl.col("distinct_relations").list.len() > 1)
    # Relations that the old design would have thrown away: every asserted
    # relation other than the single winner surfaced in `relation`.
    dropped = (
        multi.with_columns(
            pl.col("distinct_relations")
            .list.set_difference(pl.concat_list(pl.col("relation")))
            .alias("dropped")
        )
        .select(pl.col("dropped").list.len().sum())
        .item()
        or 0
    )

    conflicts = df.filter(pl.col("conflict")).height

    top = (
        multi.select(
            pl.col("distinct_relations").list.sort().list.join(" + ").alias("combo")
        )
        .group_by("combo")
        .len()
        .sort("len", descending=True)
        .head(5)
    )

    return {
        "edge": name,
        "edges_total": total,
        "edges_multi_relation": multi.height,
        "assertions_previously_dropped": dropped,
        "edges_flagged_conflicting": conflicts,
        "top_combinations": top,
    }


def main() -> None:
    """Print the before/after report for every affected edge table."""
    rows = []
    for name in EDGES:
        r = report(name)
        rows.append({k: v for k, v in r.items() if k != "top_combinations"})
        print(f"\n=== {name} ===")  # noqa: T201
        print(  # noqa: T201
            f"  edges                          : {r['edges_total']:,}\n"
            f"  edges with >1 distinct relation: {r['edges_multi_relation']:,}\n"
            f"  assertions previously dropped  : {r['assertions_previously_dropped']:,}\n"
            f"  edges flagged as conflicting   : {r['edges_flagged_conflicting']:,}"
        )
        if r["edges_multi_relation"]:
            print("  most common relation combinations:")  # noqa: T201
            for combo, n in r["top_combinations"].iter_rows():
                print(f"    {n:>10,}  {combo}")  # noqa: T201

    summary = pl.DataFrame(rows)
    print("\n=== totals ===")  # noqa: T201
    print(  # noqa: T201
        summary.select(
            pl.col("edges_total").sum(),
            pl.col("edges_multi_relation").sum(),
            pl.col("assertions_previously_dropped").sum(),
            pl.col("edges_flagged_conflicting").sum(),
        )
    )


if __name__ == "__main__":
    main()
