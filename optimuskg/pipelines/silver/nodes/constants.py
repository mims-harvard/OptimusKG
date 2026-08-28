from enum import StrEnum

import polars as pl


class Node(StrEnum):
    """3-letter abbreviations for node types in the knowledge graph."""

    ANATOMY = "ANA"
    BIOLOGICAL_PROCESS = "BPO"  # GO convention
    CELLULAR_COMPONENT = "CCO"  # GO convention
    DISEASE = "DIS"
    DRUG = "DRG"
    EXPOSURE = "EXP"
    GENE = "GEN"
    MOLECULAR_FUNCTION = "MFN"  # GO convention
    PATHWAY = "PWY"  # KEGG/Reactome convention
    PHENOTYPE = "PHE"


class Edge(StrEnum):
    @staticmethod
    def format_label(src: Node, dst: Node) -> str:
        return f"{src.value}-{dst.value}"


class Source(StrEnum):
    """Data source/database identifiers for provenance tracking.

    All values are uppercase with underscores, matching the member name.
    Native database casing is handled via aliases in ``_SOURCE_BY_RAW``.
    """

    # Direct sources
    BGEE = "BGEE"
    CTD = "CTD"
    DISGENET = "DISGENET"
    DRUG_BANK = "DRUG_BANK"
    DRUG_CENTRAL = "DRUG_CENTRAL"
    GO = "GO"
    HPO = "HPO"
    MEDDRA = "MEDDRA"
    MESH = "MESH"
    MONDO = "MONDO"
    ONSIDES = "ONSIDES"
    OPEN_TARGETS = "OPEN_TARGETS"
    PRIMEKG = "PRIMEKG"
    REACTOME = "REACTOME"
    UBERON = "UBERON"

    # Indirect sources for DisGeNET
    CGI = "CGI"
    CLINGEN = "CLINGEN"
    GENOMICS_ENGLAND = "GENOMICS_ENGLAND"
    ORPHANET = "ORPHANET"
    PSYGENET = "PSYGENET"
    UNIPROT = "UNIPROT"

    # Indirect sources from PPI databases
    APID = "APID"
    BIOGRID = "BIOGRID"
    BIOPLEX = "BIOPLEX"
    COFRAC = "COFRAC"
    ENCODE = "ENCODE"
    HIUNION = "HIUNION"
    HINT_BINARY = "HINT_BINARY"
    HINT_COMPLEX = "HINT_COMPLEX"
    HIPPIE = "HIPPIE"
    INNATEDB = "INNATEDB"
    INSIDER = "INSIDER"
    INSTRUCT = "INSTRUCT"
    INTACT = "INTACT"
    INTERACTOME3D = "INTERACTOME3D"
    INWEB = "INWEB"
    KINOMENETX = "KINOMENETX"
    LITBM17 = "LITBM17"
    MINT = "MINT"
    PHOSPHOSP = "PHOSPHOSP"
    PINA = "PINA"
    QUBIC = "QUBIC"
    SIGNALINK = "SIGNALINK"

    # Indirect sources for OpenTargets
    ATC = "ATC"
    BNF = "BNF"
    CLINICAL_TRIALS = "CLINICAL_TRIALS"
    DAILY_MED = "DAILY_MED"
    DOI = "DOI"
    EMA = "EMA"
    EXPERT = "EXPERT"
    FDA = "FDA"
    HMA = "HMA"
    INN = "INN"
    INTERPRO = "INTERPRO"
    ISBN = "ISBN"
    IUPHAR = "IUPHAR"
    KEGG = "KEGG"
    OTHER = "OTHER"
    PATENT = "PATENT"
    PMC = "PMC"
    PMDA = "PMDA"
    PUBCHEM = "PUBCHEM"
    PUBMED = "PUBMED"
    USAN = "USAN"
    WIKIPEDIA = "WIKIPEDIA"


# Mapping from raw data strings to canonical Source enum members.
# Built automatically from enum values, plus aliases for native database casing.
_SOURCE_BY_RAW: dict[str, Source] = {s.value: s for s in Source} | {
    # Native database casing aliases (lowercase, mixed-case, hyphenated, etc.)
    "biogrid": Source.BIOGRID,
    "bioplex": Source.BIOPLEX,
    "ClinicalTrials": Source.CLINICAL_TRIALS,
    "CoFrac": Source.COFRAC,
    "CTD_human": Source.CTD,
    "DailyMed": Source.DAILY_MED,
    "disgenet": Source.DISGENET,
    "drugbank": Source.DRUG_BANK,
    "drugcentral": Source.DRUG_CENTRAL,
    "encode": Source.ENCODE,
    "Expert": Source.EXPERT,
    "HINT-binary": Source.HINT_BINARY,
    "HINT-complex": Source.HINT_COMPLEX,
    "hiunion": Source.HIUNION,
    "HP": Source.HPO,
    "innatedb": Source.INNATEDB,
    "insider": Source.INSIDER,
    "instruct": Source.INSTRUCT,
    "intact": Source.INTACT,
    "interactome3d": Source.INTERACTOME3D,
    "InterPro": Source.INTERPRO,
    "inweb": Source.INWEB,
    "KinomeNetX": Source.KINOMENETX,
    "litbm17": Source.LITBM17,
    "MedDRA": Source.MEDDRA,
    "mint": Source.MINT,
    "OnSIDES": Source.ONSIDES,
    "opentargets": Source.OPEN_TARGETS,
    "Other": Source.OTHER,
    "Patent": Source.PATENT,
    "PhosphoSP": Source.PHOSPHOSP,
    "pina": Source.PINA,
    "PrimeKG": Source.PRIMEKG,
    "PubChem": Source.PUBCHEM,
    "PubMed": Source.PUBMED,
    "qubic": Source.QUBIC,
    "signalink": Source.SIGNALINK,
    "UNIPROT": Source.UNIPROT,
    "UniProt": Source.UNIPROT,
    "Wikipedia": Source.WIKIPEDIA,
}


def resolve_source(raw: str) -> str:
    """Resolve a raw source string to a canonical Source enum value.

    Args:
        raw: A source string from upstream data.

    Returns:
        The canonical Source string value.

    Raises:
        KeyError: If the raw string is not a known source or alias.
    """
    return _SOURCE_BY_RAW[raw]


def resolve_sources(sources: pl.Series) -> list[str]:
    """Resolve a list of raw source strings to canonical Source enum values.

    Intended for use with ``map_elements`` on a ``List(String)`` column.

    Args:
        sources: Polars Series of raw source strings (passed by map_elements
            when applied to a List column).

    Returns:
        List of canonical Source string values.

    Raises:
        KeyError: If any raw string is not a known source or alias.
    """
    return [resolve_source(s) for s in sources.to_list()]


class Relation(StrEnum):
    """Standardized relation types for edges in the knowledge graph.

    All values are uppercase with underscores for consistency.
    """

    # Hierarchy relations
    PARENT = "PARENT"
    IS_A = "IS_A"

    # Association/Interaction relations
    INTERACTS_WITH = "INTERACTS_WITH"
    ASSOCIATED_WITH = "ASSOCIATED_WITH"
    LINKED_TO = "LINKED_TO"

    # Expression relations (anatomy-gene)
    EXPRESSION_PRESENT = "EXPRESSION_PRESENT"
    EXPRESSION_ABSENT = "EXPRESSION_ABSENT"

    # Phenotype relations (disease-phenotype)
    PHENOTYPE_PRESENT = "PHENOTYPE_PRESENT"
    PHENOTYPE_ABSENT = "PHENOTYPE_ABSENT"

    # Drug-Disease relations
    INDICATION = "INDICATION"
    OFF_LABEL_USE = "OFF_LABEL_USE"
    CONTRAINDICATION = "CONTRAINDICATION"

    # Drug-Phenotype relations
    ADVERSE_DRUG_REACTION = "ADVERSE_DRUG_REACTION"

    # Drug-Drug relations
    SYNERGISTIC_INTERACTION = "SYNERGISTIC_INTERACTION"

    # Drug-Gene role relations
    TARGET = "TARGET"
    ENZYME = "ENZYME"
    TRANSPORTER = "TRANSPORTER"
    CARRIER = "CARRIER"

    # Drug-Gene action relations
    ACTIVATOR = "ACTIVATOR"
    AGONIST = "AGONIST"
    ALLOSTERIC_ANTAGONIST = "ALLOSTERIC_ANTAGONIST"
    ANTAGONIST = "ANTAGONIST"
    ANTISENSE_INHIBITOR = "ANTISENSE_INHIBITOR"
    BINDING_AGENT = "BINDING_AGENT"
    BLOCKER = "BLOCKER"
    CROSS_LINKING_AGENT = "CROSS_LINKING_AGENT"
    DEGRADER = "DEGRADER"
    DISRUPTING_AGENT = "DISRUPTING_AGENT"
    EXOGENOUS_GENE = "EXOGENOUS_GENE"
    EXOGENOUS_PROTEIN = "EXOGENOUS_PROTEIN"
    HYDROLYTIC_ENZYME = "HYDROLYTIC_ENZYME"
    INHIBITOR = "INHIBITOR"
    INVERSE_AGONIST = "INVERSE_AGONIST"
    MODULATOR = "MODULATOR"
    NEGATIVE_ALLOSTERIC_MODULATOR = "NEGATIVE_ALLOSTERIC_MODULATOR"
    NEGATIVE_MODULATOR = "NEGATIVE_MODULATOR"
    OPENER = "OPENER"
    OTHER = "OTHER"
    PARTIAL_AGONIST = "PARTIAL_AGONIST"
    POSITIVE_ALLOSTERIC_MODULATOR = "POSITIVE_ALLOSTERIC_MODULATOR"
    POSITIVE_MODULATOR = "POSITIVE_MODULATOR"
    PROTEOLYTIC_ENZYME = "PROTEOLYTIC_ENZYME"
    RELEASING_AGENT = "RELEASING_AGENT"
    RNAI_INHIBITOR = "RNAI_INHIBITOR"
    STABILISER = "STABILISER"
    SUBSTRATE = "SUBSTRATE"
    VACCINE_ANTIGEN = "VACCINE_ANTIGEN"


# Priority mapping for relation resolution (lower = higher priority)
# When multiple relations exist, the one with lowest priority number wins
RELATION_PRIORITY: dict[Relation, int] = {
    # Drug-Disease priorities
    Relation.INDICATION: 1,  # Most specific
    Relation.CONTRAINDICATION: 2,
    Relation.OFF_LABEL_USE: 3,
    # Drug-Drug priorities
    Relation.SYNERGISTIC_INTERACTION: 1,  # Specific interaction
    Relation.PARENT: 10,  # Generic hierarchy
    # Drug-Protein action types (all equal, most specific)
    Relation.ACTIVATOR: 1,
    Relation.AGONIST: 1,
    Relation.ALLOSTERIC_ANTAGONIST: 1,
    Relation.ANTAGONIST: 1,
    Relation.ANTISENSE_INHIBITOR: 1,
    Relation.BINDING_AGENT: 1,
    Relation.BLOCKER: 1,
    Relation.CROSS_LINKING_AGENT: 1,
    Relation.DEGRADER: 1,
    Relation.DISRUPTING_AGENT: 1,
    Relation.EXOGENOUS_GENE: 1,
    Relation.EXOGENOUS_PROTEIN: 1,
    Relation.HYDROLYTIC_ENZYME: 1,
    Relation.INHIBITOR: 1,
    Relation.INVERSE_AGONIST: 1,
    Relation.MODULATOR: 1,
    Relation.NEGATIVE_ALLOSTERIC_MODULATOR: 1,
    Relation.NEGATIVE_MODULATOR: 1,
    Relation.OPENER: 1,
    Relation.PARTIAL_AGONIST: 1,
    Relation.POSITIVE_ALLOSTERIC_MODULATOR: 1,
    Relation.POSITIVE_MODULATOR: 1,
    Relation.PROTEOLYTIC_ENZYME: 1,
    Relation.RELEASING_AGENT: 1,
    Relation.RNAI_INHIBITOR: 1,
    Relation.STABILISER: 1,
    Relation.SUBSTRATE: 1,
    Relation.VACCINE_ANTIGEN: 1,
    # Drug-Protein role types (less specific than actions)
    Relation.ENZYME: 10,
    Relation.TRANSPORTER: 11,
    Relation.CARRIER: 12,
    Relation.TARGET: 13,
    Relation.OTHER: 100,  # Least specific
    # Other relation types (default priorities)
    Relation.IS_A: 1,
    Relation.INTERACTS_WITH: 1,
    Relation.ASSOCIATED_WITH: 1,
    Relation.LINKED_TO: 1,
    Relation.EXPRESSION_PRESENT: 1,
    Relation.EXPRESSION_ABSENT: 1,
    Relation.PHENOTYPE_PRESENT: 1,
    Relation.PHENOTYPE_ABSENT: 1,
    Relation.ADVERSE_DRUG_REACTION: 1,
}


# Relation pairs/groups that are mutually exclusive statements about the same
# node pair. When two or more members of the same group are asserted for one
# node pair, the edge carries genuinely conflicting evidence and is flagged via
# the ``relation_conflict`` property so downstream users can handle it
# explicitly instead of silently trusting the collapsed ``relation`` value.
MUTUALLY_EXCLUSIVE_RELATIONS: tuple[frozenset[Relation], ...] = (
    frozenset({Relation.INDICATION, Relation.CONTRAINDICATION, Relation.OFF_LABEL_USE}),
    frozenset({Relation.PHENOTYPE_PRESENT, Relation.PHENOTYPE_ABSENT}),
    frozenset({Relation.EXPRESSION_PRESENT, Relation.EXPRESSION_ABSENT}),
    frozenset({Relation.AGONIST, Relation.ANTAGONIST, Relation.INVERSE_AGONIST}),
    frozenset({Relation.ACTIVATOR, Relation.INHIBITOR}),
    frozenset({Relation.POSITIVE_MODULATOR, Relation.NEGATIVE_MODULATOR}),
    frozenset(
        {
            Relation.POSITIVE_ALLOSTERIC_MODULATOR,
            Relation.NEGATIVE_ALLOSTERIC_MODULATOR,
        }
    ),
)

# Struct describing a single source-specific relation assertion. The full list
# of assertions is preserved on every collapsed edge so that no original
# statement is lost by the one-edge-per-node-pair invariant.
RELATION_ASSERTION_DTYPE = pl.Struct({"source": pl.String, "relation": pl.String})
RELATION_ASSERTIONS_DTYPE = pl.List(RELATION_ASSERTION_DTYPE)

# Zero-padded priorities used to build a sortable "priority|relation" key so
# that resolution is a pure Polars expression (no per-row Python callback).
_RELATION_SORT_KEY: dict[str, str] = {
    r.value: f"{RELATION_PRIORITY.get(r, 999):03d}|{r.value}" for r in Relation
}
_DEFAULT_SORT_KEY = f"999|{Relation.OTHER.value}"

# Two or more members of the same mutually exclusive group means a conflict.
_CONFLICT_THRESHOLD = 2


def relation_assertions(source: Source, relation: pl.Expr) -> pl.Expr:
    """Tag a relation column with the source that asserted it.

    Args:
        source: The dataset that made these assertions.
        relation: Expression yielding either a ``String`` relation or a
            ``List(String)`` of relations for the node pair.

    Returns:
        Expression of dtype ``RELATION_ASSERTIONS_DTYPE`` (list of
        ``{source, relation}`` structs), never null (empty list instead).
    """
    return (
        pl.concat_list(relation)
        .list.drop_nulls()
        .list.eval(
            pl.struct(
                pl.lit(str(source), dtype=pl.String).alias("source"),
                pl.element().cast(pl.String).alias("relation"),
            )
        )
        .fill_null(pl.lit([], dtype=RELATION_ASSERTIONS_DTYPE))
        .cast(RELATION_ASSERTIONS_DTYPE)
    )


def merge_relation_assertions(*assertions: pl.Expr) -> pl.Expr:
    """Concatenate assertion lists from several sources, de-duplicated and sorted.

    Args:
        *assertions: Expressions of dtype ``RELATION_ASSERTIONS_DTYPE``. Null
            values are treated as empty lists so that outer joins are safe.

    Returns:
        A single deterministic assertion list expression.
    """
    filled = [
        a.fill_null(pl.lit([], dtype=RELATION_ASSERTIONS_DTYPE)).cast(
            RELATION_ASSERTIONS_DTYPE
        )
        for a in assertions
    ]
    return pl.concat_list(filled).list.unique().list.sort()


def resolve_relation_expr(assertions: pl.Expr) -> pl.Expr:
    """Pick the representative relation for an edge from its assertions.

    Resolution is deterministic: lowest ``RELATION_PRIORITY`` wins, ties are
    broken alphabetically. This only chooses which value is surfaced in the
    ``relation`` column; every asserted relation remains available in the
    ``relation_assertions`` property.

    Args:
        assertions: Expression of dtype ``RELATION_ASSERTIONS_DTYPE``.

    Returns:
        String expression with the representative ``Relation`` value.
    """
    return (
        assertions.list.eval(
            pl.element()
            .struct.field("relation")
            .replace_strict(_RELATION_SORT_KEY, default=_DEFAULT_SORT_KEY)
        )
        .list.min()
        .str.split("|")
        .list.last()
        .fill_null(pl.lit(str(Relation.OTHER)))
    )


def relation_conflict_expr(assertions: pl.Expr) -> pl.Expr:
    """Flag edges whose assertions contain mutually exclusive relations.

    Args:
        assertions: Expression of dtype ``RELATION_ASSERTIONS_DTYPE``.

    Returns:
        Boolean expression, ``True`` when two or more members of the same
        mutually exclusive group are asserted for the node pair (for example an
        ``INDICATION`` and a ``CONTRAINDICATION`` between the same drug and
        disease).
    """
    distinct = assertions.list.eval(pl.element().struct.field("relation")).list.unique()
    checks = [
        distinct.list.eval(
            pl.element().is_in(sorted(str(r) for r in group)).cast(pl.UInt32)
        ).list.sum()
        >= _CONFLICT_THRESHOLD
        for group in MUTUALLY_EXCLUSIVE_RELATIONS
    ]
    combined = checks[0]
    for check in checks[1:]:
        combined = combined | check
    return combined.fill_null(False)


def resolve_relation(relations: pl.Series) -> str:
    """Resolve multiple Relation values to the highest priority one.

    Deprecated in favour of :func:`resolve_relation_expr`, which operates on
    provenance-carrying assertions instead of bare relation strings. Kept for
    ad-hoc use and for parity testing against the expression implementation.

    Args:
        relations: Polars Series of Relation enum string values (passed by map_elements
            when applied to a List column). All values should be valid Relation enum values.
            Unknown values will be mapped to OTHER.

    Returns:
        The Relation value with highest priority.
        If multiple relations have the same priority, returns the first alphabetically.
    """
    relation_list = relations.to_list()

    if not relation_list:
        return Relation.OTHER

    mapped: list[Relation] = []
    for r in relation_list:
        try:
            mapped.append(Relation(r))
        except ValueError:
            mapped.append(Relation.OTHER)

    if len(mapped) == 1:
        return mapped[0]

    # Sort by priority (ascending), then alphabetically by value for tie-breaking
    mapped.sort(key=lambda r: (RELATION_PRIORITY.get(r, 999), r))
    return mapped[0]
