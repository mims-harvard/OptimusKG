"""Audit every silver edge table for relation information lost at collapse time.

For each edge table, recomputes the node-pair grouping from the persisted
output and reports whether any node pair carries more than one distinct
relation. Tables that already expose ``relation_assertions`` are reported as
covered; tables where a multi-relation pair exists without that property are
flagged as gaps.

Usage:
    uv run python scripts/audit_relation_loss.py
"""

from pathlib import Path

import polars as pl

EDGES_DIR = Path("data/silver/edges")


# Edge tables that carry more than one relation type but where each node pair
# is asserted exactly once upstream, so no assertion can be lost. Each entry
# maps to a checker that proves this against the bronze input.
_VERIFIED_SINGLE_ASSERTION = {"anatomy_gene"}


def _verify_single_assertion(name: str) -> bool:
    """Prove that the upstream source asserts one relation per node pair."""
    if name != "anatomy_gene":
        return False
    src = pl.read_parquet("data/bronze/bgee/gene_expressions_in_anatomy.parquet")
    grouped = (
        src.filter(pl.col("expression").is_in(["present", "absent"]))
        .select(
            pl.col("anatomy_id").str.replace("UBERON:", "UBERON_").alias("f"),
            pl.col("gene_id").alias("t"),
            pl.col("expression").alias("e"),
        )
        .group_by(["f", "t"])
        .agg(pl.col("e").n_unique().alias("n"))
    )
    return grouped.filter(pl.col("n") > 1).height == 0


def main() -> None:
    """Report per-edge-table relation coverage and any remaining gaps."""
    gaps: list[str] = []
    rows: list[dict[str, object]] = []

    for path in sorted(EDGES_DIR.glob("*.parquet")):
        name = path.stem
        df = pl.read_parquet(path)
        props = df.schema["properties"]
        prop_names = set(dict(props).keys()) if isinstance(props, pl.Struct) else set()
        covered = "relation_assertions" in prop_names

        distinct_relations = df.select(pl.col("relation").n_unique()).item()

        if covered:
            multi = (
                df.select(
                    pl.col("properties")
                    .struct.field("relation_assertions")
                    .list.eval(pl.element().struct.field("relation"))
                    .list.unique()
                    .list.len()
                    .alias("n")
                )
                .filter(pl.col("n") > 1)
                .height
            )
            conflicts = df.filter(
                pl.col("properties").struct.field("relation_conflict")
            ).height
        else:
            multi = 0
            conflicts = 0
            # No assertions stored. This is only safe if the upstream source
            # cannot assert two different relations for one node pair. Verify
            # that directly against the bronze input instead of assuming it.
            if distinct_relations > 1:
                if name in _VERIFIED_SINGLE_ASSERTION and _verify_single_assertion(
                    name
                ):
                    pass
                else:
                    gaps.append(name)

        rows.append(
            {
                "edge": name,
                "edges": df.height,
                "distinct_relations": distinct_relations,
                "assertions_kept": covered,
                "multi_relation_edges": multi,
                "conflicting_edges": conflicts,
            }
        )

    summary = pl.DataFrame(rows)
    with pl.Config(tbl_rows=-1, fmt_str_lengths=40):
        print(summary)  # noqa: T201

    print(  # noqa: T201
        f"\ntotals: {summary['edges'].sum():,} edges, "
        f"{summary['multi_relation_edges'].sum():,} multi-relation, "
        f"{summary['conflicting_edges'].sum():,} conflicting"
    )

    if gaps:
        print(f"\nGAPS (multi-relation table without assertions): {gaps}")  # noqa: T201
    else:
        print(  # noqa: T201
            "\nNo gaps: every edge table with more than one relation type "
            "retains its source-specific assertions."
        )


if __name__ == "__main__":
    main()
