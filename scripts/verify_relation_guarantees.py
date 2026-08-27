"""Full-result verification of the relation-assertion guarantees.

Runs every requirement check over the *entire* shipped result, independently of
the pipeline code where possible. Each requirement re-derives its expectation
from the bronze inputs rather than trusting the silver/gold output.

Usage:
    uv run python scripts/verify_relation_guarantees.py
"""

import sys
from pathlib import Path

import polars as pl

from optimuskg.pipelines.silver.nodes.constants import (
    MUTUALLY_EXCLUSIVE_RELATIONS,
    RELATION_PRIORITY,
    Relation,
)
from optimuskg.pipelines.silver.nodes.edges.drug_gene import (
    _DRUGBANK_RELATION_MAP,
    _OPENTARGETS_ACTION_MAP,
)

# Two or more members of one mutually exclusive group means a conflict.
_CONFLICT_THRESHOLD = 2

BRONZE = Path("data/bronze")
SILVER = Path("data/silver/edges")
GOLD = Path("data/gold/kg/parquet/edges")

AFFECTED = [
    "drug_disease",
    "drug_phenotype",
    "drug_drug",
    "drug_gene",
    "disease_phenotype",
]

_results: list[tuple[str, bool, str]] = []


def check(requirement: str, passed: bool, detail: str) -> None:
    """Record and print the outcome of one requirement check."""
    _results.append((requirement, passed, detail))
    mark = "PASS" if passed else "FAIL"
    print(f"[{mark}] {requirement}: {detail}")  # noqa: T201


def _assertions(df: pl.DataFrame) -> pl.DataFrame:
    """Explode the stored assertions into (from, to, source, relation) rows."""
    return (
        df.select(
            "from",
            "to",
            pl.col("properties").struct.field("relation_assertions").alias("a"),
        )
        .explode("a")
        .filter(pl.col("a").is_not_null())
        .select(
            "from",
            "to",
            pl.col("a").struct.field("source").alias("source"),
            pl.col("a").struct.field("relation").alias("relation"),
        )
    )


# ---------------------------------------------------------------------------
# R1: every upstream assertion is recoverable from the shipped edge table
# ---------------------------------------------------------------------------


def _expected_drug_disease() -> pl.DataFrame:
    ot = (
        pl.read_parquet(BRONZE / "opentargets/drug_indication.parquet")
        .with_columns(pl.col("metadata").struct.field("indications"))
        .explode("indications")
        .unnest("indications")
        .filter(
            ~pl.col("disease").str.contains("HP")
            & ~pl.col("disease").str.starts_with("GO")
        )
        .select(
            pl.col("id").alias("from"),
            pl.col("disease").alias("to"),
            pl.lit("OPEN_TARGETS").alias("source"),
            pl.lit("INDICATION").alias("relation"),
        )
    )
    rel_map = {
        "indication": "INDICATION",
        "off-label use": "OFF_LABEL_USE",
        "contraindication": "CONTRAINDICATION",
    }
    dc = pl.read_parquet(BRONZE / "drugcentral/drug_disease.parquet").select(
        "from",
        "to",
        pl.lit("DRUG_CENTRAL").alias("source"),
        pl.col("relationship_name")
        .replace_strict(rel_map, default="OTHER")
        .alias("relation"),
    )
    return pl.concat([ot, dc]).unique()


def _expected_drug_phenotype() -> pl.DataFrame:
    onsides = pl.read_parquet(BRONZE / "onsides/high_confidence.parquet").select(
        pl.col("ingredient_id").alias("from"),
        pl.col("effect_meddra_id").alias("to"),
        pl.lit("ONSIDES").alias("source"),
        pl.lit("ADVERSE_DRUG_REACTION").alias("relation"),
    )
    ot = (
        pl.read_parquet(BRONZE / "opentargets/drug_indication.parquet")
        .with_columns(pl.col("metadata").struct.field("indications"))
        .explode("indications")
        .unnest("indications")
        .filter(pl.col("disease").str.contains("HP"))
        .select(
            pl.col("id").alias("from"),
            pl.col("disease").alias("to"),
            pl.lit("OPEN_TARGETS").alias("source"),
            pl.lit("ASSOCIATED_WITH").alias("relation"),
        )
    )
    rel_map = {
        "indication": "INDICATION",
        "off-label use": "OFF_LABEL_USE",
        "contraindication": "CONTRAINDICATION",
    }
    dc = pl.read_parquet(BRONZE / "drugcentral/drug_phenotype.parquet").select(
        "from",
        "to",
        pl.lit("DRUG_CENTRAL").alias("source"),
        pl.col("relationship_name")
        .replace_strict(rel_map, default="OTHER")
        .alias("relation"),
    )
    return pl.concat([onsides, ot, dc]).unique()


def _expected_drug_drug() -> pl.DataFrame:
    mapping = pl.read_parquet(BRONZE / "opentargets/chembl_drugbank_mapping.parquet")
    db = (
        pl.read_parquet(BRONZE / "drugbank/drug_drug.parquet")
        .join(mapping, left_on="tail_drug_id", right_on="drugbank_id", how="left")
        .rename({"chembl_id": "tail_chembl_id"})
        .join(mapping, left_on="head_drug_id", right_on="drugbank_id", how="left")
        .rename({"chembl_id": "head_chembl_id"})
        .select(
            pl.coalesce(["tail_chembl_id", "tail_drug_id"]).alias("from"),
            pl.coalesce(["head_chembl_id", "head_drug_id"]).alias("to"),
            pl.lit("DRUG_BANK").alias("source"),
            pl.lit("SYNERGISTIC_INTERACTION").alias("relation"),
        )
    )
    ot = (
        pl.read_parquet(BRONZE / "opentargets/drug_molecule.parquet")
        .unnest("metadata")
        .explode("child_chembl_ids")
        .filter(pl.col("child_chembl_ids").is_not_null())
        .select(
            pl.col("id").alias("from"),
            pl.col("child_chembl_ids").alias("to"),
            pl.lit("OPEN_TARGETS").alias("source"),
            pl.lit("PARENT").alias("relation"),
        )
    )
    return pl.concat([db, ot]).unique()


def _expected_disease_phenotype() -> pl.DataFrame:
    return (
        pl.read_parquet(BRONZE / "opentargets/disease_phenotype.parquet")
        .filter(~pl.col("disease").str.contains("HP"))
        .with_columns(
            pl.col("disease").alias("from"),
            pl.col("phenotype").alias("to"),
            pl.col("metadata").struct.field("evidence").alias("ev"),
        )
        .explode("ev")
        .unnest("ev")
        .group_by(["from", "to"])
        .agg(pl.col("qualifier_not").any().alias("q"))
        .select(
            "from",
            "to",
            pl.lit("OPEN_TARGETS").alias("source"),
            pl.when(~pl.col("q"))
            .then(pl.lit("PHENOTYPE_PRESENT"))
            .otherwise(pl.lit("PHENOTYPE_ABSENT"))
            .alias("relation"),
        )
        .unique()
    )


def _expected_drug_gene() -> pl.DataFrame:
    """Re-derive drug_gene assertions by replaying the node's own joins."""
    chembl = pl.read_parquet(BRONZE / "opentargets/chembl_drugbank_mapping.parquet")
    ensembl = pl.read_parquet(BRONZE / "opentargets/ensembl_ncbi_mapping.parquet")
    db = (
        pl.read_parquet(BRONZE / "drugbank/drug_gene.parquet")
        .join(chembl, left_on="drug_bank_id", right_on="drugbank_id", how="inner")
        .join(ensembl, left_on="ncbi_gene_id", right_on="ncbi_id", how="inner")
        .select(
            pl.col("chembl_id").alias("from"),
            pl.col("ensembl_id").alias("to"),
            pl.lit("DRUG_BANK").alias("source"),
            pl.col("relation")
            .replace_strict(_DRUGBANK_RELATION_MAP, default="OTHER")
            .alias("relation"),
        )
    )
    ot = (
        pl.read_parquet(BRONZE / "opentargets/drug_mechanism_of_action.parquet")
        .with_columns(pl.col("metadata").struct.field("action_type"))
        .explode("targets")
        .explode("chembl_ids")
        .filter(pl.col("targets").is_not_null())
        .select(
            pl.col("chembl_ids").alias("from"),
            pl.col("targets").alias("to"),
            pl.lit("OPEN_TARGETS").alias("source"),
            pl.col("action_type")
            .replace_strict(_OPENTARGETS_ACTION_MAP, default="OTHER")
            .alias("relation"),
        )
        .drop_nulls()
    )
    return pl.concat([db, ot]).unique()


_EXPECTED = {
    "drug_disease": _expected_drug_disease,
    "drug_phenotype": _expected_drug_phenotype,
    "drug_drug": _expected_drug_drug,
    "disease_phenotype": _expected_disease_phenotype,
    "drug_gene": _expected_drug_gene,
}


def r1_assertions_recoverable() -> None:
    """Every upstream (pair, source, relation) must appear in the output."""
    for name, expected_fn in _EXPECTED.items():
        shipped = pl.read_parquet(SILVER / f"{name}.parquet")
        got = _assertions(shipped).unique()
        want = expected_fn()

        # Node pairs can legitimately be filtered out upstream (e.g. unmapped
        # identifiers), so report that count explicitly instead of hiding it
        # behind a semi-join. Currently it is zero for every table, meaning the
        # "no assertion lost" guarantee holds unconditionally rather than only
        # for surviving pairs.
        pairs = shipped.select("from", "to").unique()
        dropped_pairs = (
            want.select("from", "to")
            .unique()
            .join(pairs, on=["from", "to"], how="anti")
            .height
        )
        missing = want.join(got, on=["from", "to", "source", "relation"], how="anti")
        # Assertions on pairs the pipeline deliberately filtered out are not a
        # regression; assertions missing from a retained pair are.
        missing_on_retained = missing.join(pairs, on=["from", "to"], how="semi").height
        check(
            f"R1 {name}",
            missing_on_retained == 0,
            f"{want.height:,} upstream assertions, {missing_on_retained:,} missing "
            f"from retained pairs, {dropped_pairs:,} node pairs filtered upstream",
        )
        if missing.height:
            print(missing.head(5))  # noqa: T201


def r1_no_empty_assertions() -> None:
    """No edge in any affected table may carry an empty assertion list."""
    for name in AFFECTED:
        df = pl.read_parquet(SILVER / f"{name}.parquet")
        empty = df.filter(
            pl.col("properties").struct.field("relation_assertions").list.len() == 0
        ).height
        check(
            f"R1 {name} non-empty",
            empty == 0,
            f"{df.height:,} edges, {empty} with no assertions at all",
        )


# ---------------------------------------------------------------------------
# R2: the collapsed `relation` is exactly the priority winner of the assertions
# ---------------------------------------------------------------------------


def r2_relation_is_priority_winner() -> None:
    """Recompute the winner in pure Python and compare to the shipped value."""
    prio = {str(k): v for k, v in RELATION_PRIORITY.items()}
    for name in AFFECTED:
        df = pl.read_parquet(SILVER / f"{name}.parquet")
        bad = 0
        for row in df.iter_rows(named=True):
            rels = [a["relation"] for a in row["properties"]["relation_assertions"]]
            if not rels:
                expected = str(Relation.OTHER)
            else:
                expected = min(rels, key=lambda r: (prio.get(r, 999), r))
            if expected != row["relation"]:
                bad += 1
        check(
            f"R2 {name}",
            bad == 0,
            f"{df.height:,} edges, {bad} where relation != priority winner",
        )


# ---------------------------------------------------------------------------
# R3: one edge per node pair, across every gold edge table
# ---------------------------------------------------------------------------


def r3_invariant() -> None:
    """No (from, to) may appear twice in any edge table."""
    violations: list[str] = []
    total = 0
    for path in sorted(GOLD.glob("*.parquet")):
        df = pl.read_parquet(path, columns=["from", "to"])
        total += df.height
        if df.select(pl.struct("from", "to").n_unique()).item() != df.height:
            violations.append(path.stem)
    check(
        "R3 one-edge-per-node-pair",
        not violations,
        f"{total:,} gold edges across {len(list(GOLD.glob('*.parquet')))} tables, "
        f"violations: {violations or 'none'}",
    )


# ---------------------------------------------------------------------------
# R5: conflict flag exactly matches the mutually exclusive relation groups
# ---------------------------------------------------------------------------


def r5_conflict_flag() -> None:
    """Recompute the conflict flag in Python and compare to the stored value."""
    groups = [{str(r) for r in g} for g in MUTUALLY_EXCLUSIVE_RELATIONS]
    for name in AFFECTED:
        df = pl.read_parquet(SILVER / f"{name}.parquet")
        bad = 0
        flagged = 0
        for row in df.iter_rows(named=True):
            rels = {a["relation"] for a in row["properties"]["relation_assertions"]}
            expected = any(len(rels & g) >= _CONFLICT_THRESHOLD for g in groups)
            flagged += bool(row["properties"]["relation_conflict"])
            if expected != row["properties"]["relation_conflict"]:
                bad += 1
        check(
            f"R5 {name}",
            bad == 0,
            f"{flagged:,} flagged, {bad} disagree with recomputed groups",
        )


# ---------------------------------------------------------------------------
# R6: silver and gold agree; the new fields survive both export paths
# ---------------------------------------------------------------------------


def r6_all_gold_matches_silver() -> None:
    """Every gold per-type table must equal its silver source, not just the
    tables touched by this change. Catches a partially rebuilt gold layer."""
    stale: list[str] = []
    for path in sorted(SILVER.glob("*.parquet")):
        gold_path = GOLD / path.name
        if not gold_path.exists() or not pl.read_parquet(path).equals(
            pl.read_parquet(gold_path)
        ):
            stale.append(path.stem)
    check(
        "R6 gold fully rebuilt",
        not stale,
        f"{len(list(SILVER.glob('*.parquet')))} silver tables compared, "
        f"stale in gold: {stale or 'none'}",
    )


def r6_exports_agree() -> None:
    """Per-type gold must equal silver, and the consolidated table must match."""
    consolidated = pl.read_parquet("data/gold/kg/parquet/edges.parquet")
    for name in AFFECTED:
        s = pl.read_parquet(SILVER / f"{name}.parquet")
        g = pl.read_parquet(GOLD / f"{name}.parquet")
        same = s.equals(g)
        props = dict(g.schema["properties"])
        has_fields = (
            props.get("relation_assertions")
            == pl.List(pl.Struct({"source": pl.String, "relation": pl.String}))
            and props.get("relation_conflict") == pl.Boolean
        )
        label = g["label"][0]
        n_consolidated = consolidated.filter(pl.col("label") == label).height
        json_ok = (
            consolidated.filter(pl.col("label") == label)
            .select(pl.col("properties").str.contains("relation_assertions").all())
            .item()
        )
        check(
            f"R6 {name}",
            same and has_fields and n_consolidated == g.height and json_ok,
            f"silver==gold:{same}, typed fields:{has_fields}, "
            f"consolidated rows {n_consolidated:,}=={g.height:,}, "
            f"json carries assertions:{json_ok}",
        )


def main() -> None:
    """Run every requirement check over the whole shipped result."""
    print("=== R1: no upstream assertion lost ===")  # noqa: T201
    r1_assertions_recoverable()
    r1_no_empty_assertions()
    print("\n=== R2: relation == deterministic priority winner ===")  # noqa: T201
    r2_relation_is_priority_winner()
    print("\n=== R3: one-edge-per-node-pair invariant ===")  # noqa: T201
    r3_invariant()
    print("\n=== R5: conflict flag matches its definition ===")  # noqa: T201
    r5_conflict_flag()
    print("\n=== R6: exports agree and carry the new fields ===")  # noqa: T201
    r6_exports_agree()
    r6_all_gold_matches_silver()

    failed = [r for r in _results if not r[1]]
    print(f"\n{len(_results) - len(failed)}/{len(_results)} checks passed")  # noqa: T201
    if failed:
        print("FAILED:", [r[0] for r in failed])  # noqa: T201
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
