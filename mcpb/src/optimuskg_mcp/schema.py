"""Type-code and JSON-key vocabulary shared by the index builder and the tools."""

from __future__ import annotations

# Node ``label`` code -> human-readable type name.
NODE_TYPE_CODES: dict[str, str] = {
    "GEN": "gene",
    "DRG": "drug",
    "DIS": "disease",
    "PHE": "phenotype",
    "ANA": "anatomy",
    "PWY": "pathway",
    "EXP": "exposure",
    "BPO": "biological_process",
    "CCO": "cellular_component",
    "MFN": "molecular_function",
}

# Numeric edge "strength" keys, in priority order. Only association edges
# (DIS-GEN / PHE-GEN) carry one; structural edges get a NULL score.
EDGE_SCORE_KEYS: tuple[str, ...] = (
    "evidence_score",
    "disgenet_score",
    "combined_score",
    "score",
)

# JSON keys that may hold a display name, in priority order.
NAME_KEYS: tuple[str, ...] = ("symbol", "name")

# JSON keys that hold synonym lists; concatenated into the search blob.
SYNONYM_KEYS: tuple[str, ...] = (
    "synonyms",
    "exact_synonyms",
    "related_synonyms",
    "narrow_synonyms",
    "broad_synonyms",
)


def node_type_case_sql(code_column: str) -> str:
    """Return a SQL ``CASE`` expression decoding a code column to a type name."""
    whens = "\n".join(
        f"    WHEN {code_column} = '{code}' THEN '{name}'"
        for code, name in NODE_TYPE_CODES.items()
    )
    return f"CASE\n{whens}\n    ELSE lower({code_column})\nEND"
