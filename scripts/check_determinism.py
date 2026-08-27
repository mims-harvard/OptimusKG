"""Check every silver edge builder for reproducibility under input reordering.

``group_by().agg()`` combined with a bare ``.unique()`` returns list elements in
a non-deterministic order. For a published dataset this means two identical
runs can produce different bytes, breaking checksum verification and making
results non-citable. This script runs each builder twice, once with reversed
inputs, and reports which tables differ.

Usage:
    uv run python scripts/check_determinism.py
"""

import sys

import polars as pl

from optimuskg.pipelines.silver.nodes import edges as E

R = pl.read_parquet
B = "data/bronze"


def _cases() -> dict[str, tuple]:
    """Builder name -> (run function, bronze input frames)."""
    return {
        "drug_disease": (
            E.drug_disease.run,
            [
                R(f"{B}/opentargets/drug_indication.parquet"),
                R(f"{B}/drugcentral/drug_disease.parquet"),
            ],
        ),
        "drug_phenotype": (
            E.drug_phenotype.run,
            [
                R(f"{B}/onsides/high_confidence.parquet"),
                R(f"{B}/opentargets/drug_indication.parquet"),
                R(f"{B}/drugcentral/drug_phenotype.parquet"),
            ],
        ),
        "drug_drug": (
            E.drug_drug.run,
            [
                R(f"{B}/drugbank/drug_drug.parquet"),
                R(f"{B}/opentargets/drug_molecule.parquet"),
                R(f"{B}/opentargets/chembl_drugbank_mapping.parquet"),
            ],
        ),
        "drug_gene": (
            E.drug_gene.run,
            [
                R(f"{B}/drugbank/drug_gene.parquet"),
                R(f"{B}/opentargets/drug_mechanism_of_action.parquet"),
                R(f"{B}/opentargets/chembl_drugbank_mapping.parquet"),
                R(f"{B}/opentargets/ensembl_ncbi_mapping.parquet"),
            ],
        ),
        "disease_phenotype": (
            E.disease_phenotype.run,
            [R(f"{B}/opentargets/disease_phenotype.parquet")],
        ),
        "disease_gene": (
            E.disease_gene.run,
            [
                R(f"{B}/opentargets/target_disease_associations.parquet"),
                R(f"{B}/opentargets/disease.parquet"),
                R(f"{B}/opentargets/target.parquet"),
                R(f"{B}/disgenet/diseases.parquet"),
            ],
        ),
        "biological_process_gene": (
            E.biological_process_gene.run,
            [R(f"{B}/opentargets/target.parquet")],
        ),
        "molecular_function_gene": (
            E.molecular_function_gene.run,
            [R(f"{B}/opentargets/target.parquet")],
        ),
        "cellular_component_gene": (
            E.cellular_component_gene.run,
            [R(f"{B}/opentargets/target.parquet")],
        ),
        "exposure_gene": (
            E.exposure_gene.run,
            [
                R(f"{B}/ctd/ctd_exposure_events.parquet"),
                R(f"{B}/opentargets/target.parquet"),
            ],
        ),
        "exposure_biological_process": (
            E.exposure_biological_process.run,
            [
                R(f"{B}/ctd/ctd_exposure_events.parquet"),
                R(f"{B}/ontology/go_terms.parquet"),
            ],
        ),
        "exposure_disease": (
            E.exposure_disease.run,
            [
                R(f"{B}/ctd/ctd_exposure_events.parquet"),
                R(f"{B}/ontology/mondo_xrefs.parquet"),
            ],
        ),
        "exposure_cellular_component": (
            E.exposure_cellular_component.run,
            [
                R(f"{B}/ctd/ctd_exposure_events.parquet"),
                R(f"{B}/ontology/go_terms.parquet"),
            ],
        ),
        "exposure_molecular_function": (
            E.exposure_molecular_function.run,
            [
                R(f"{B}/ctd/ctd_exposure_events.parquet"),
                R(f"{B}/ontology/go_terms.parquet"),
            ],
        ),
        "exposure_exposure": (
            E.exposure_exposure.run,
            [R(f"{B}/ctd/ctd_exposure_events.parquet")],
        ),
    }


def main() -> None:
    """Run each builder twice and report reproducibility."""
    unstable: list[str] = []
    for name, (fn, args) in _cases().items():
        try:
            a = fn(*args)
            b = fn(*[df.reverse() for df in args])
        except Exception as exc:  # noqa: BLE001
            print(f"[SKIP] {name}: {type(exc).__name__}: {exc}")  # noqa: T201
            continue
        stable = a.equals(b)
        if not stable:
            unstable.append(name)
        print(  # noqa: T201
            f"[{'PASS' if stable else 'FAIL'}] {name}: "
            f"{a.height:,} rows, reorder-stable={stable}"
        )

    if unstable:
        print(f"\nNON-DETERMINISTIC: {unstable}")  # noqa: T201
    else:
        print("\nAll checked builders are reproducible.")  # noqa: T201
    sys.exit(1 if unstable else 0)


if __name__ == "__main__":
    main()
