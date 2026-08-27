"""Verify the collapsed ``relation`` column is byte-for-byte unchanged.

Runs the pre-change edge builders (from a git worktree of the previous commit)
against the same bronze inputs and diffs the resulting ``relation`` values
against the current implementation.

Usage:
    uv run python scripts/compare_relation_before_after.py /tmp/okg_before
"""

import importlib.util
import sys
from pathlib import Path

import polars as pl

BRONZE = Path("data/bronze")

# edge name -> (old module relative path, bronze inputs in run() order)
CASES: dict[str, tuple[str, list[str]]] = {
    "drug_disease": (
        "drug_disease.py",
        ["opentargets/drug_indication", "drugcentral/drug_disease"],
    ),
    "drug_drug": (
        "drug_drug.py",
        [
            "drugbank/drug_drug",
            "opentargets/drug_molecule",
            "opentargets/chembl_drugbank_mapping",
        ],
    ),
    "drug_gene": (
        "drug_gene.py",
        [
            "drugbank/drug_gene",
            "opentargets/drug_mechanism_of_action",
            "opentargets/chembl_drugbank_mapping",
            "opentargets/ensembl_ncbi_mapping",
        ],
    ),
    "disease_phenotype": (
        "disease_phenotype.py",
        ["opentargets/disease_phenotype"],
    ),
    "drug_phenotype": (
        "drug_phenotype.py",
        [
            "onsides/high_confidence",
            "opentargets/drug_indication",
            "drugcentral/drug_phenotype",
        ],
    ),
}


def _load_old(worktree: Path, rel: str, name: str):
    """Import the pre-change node module from the worktree."""
    path = worktree / "optimuskg/pipelines/silver/nodes/edges" / rel
    spec = importlib.util.spec_from_file_location(f"old_{name}", path)
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def main() -> None:
    """Diff old vs new ``relation`` values for every affected edge table."""
    worktree = Path(sys.argv[1])
    # The worktree's edge modules import from the installed optimuskg package,
    # which is the *new* constants module. Both expose resolve_relation, so the
    # old code paths still work unchanged.
    ok = True
    for name, (rel, inputs) in CASES.items():
        old = _load_old(worktree, rel, name)
        args = [pl.read_parquet(BRONZE / f"{p}.parquet") for p in inputs]
        before = old.run(*args).select("from", "to", "relation").sort("from", "to")
        after = (
            pl.read_parquet(f"data/silver/edges/{name}.parquet")
            .select("from", "to", "relation")
            .sort("from", "to")
        )

        same_shape = before.shape == after.shape
        joined = before.join(after, on=["from", "to"], how="full", suffix="_new")
        mismatches = joined.filter(
            (pl.col("relation") != pl.col("relation_new"))
            | pl.col("relation").is_null()
            | pl.col("relation_new").is_null()
        )
        status = "OK " if (same_shape and mismatches.is_empty()) else "DIFF"
        ok &= same_shape and mismatches.is_empty()
        print(  # noqa: T201
            f"[{status}] {name:<18} rows before={before.height:>9,} "
            f"after={after.height:>9,} relation mismatches={mismatches.height:,}"
        )
        if not mismatches.is_empty():
            print(mismatches.head(10))  # noqa: T201

    print("\nAll relation values identical." if ok else "\nDIFFERENCES FOUND.")  # noqa: T201
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
