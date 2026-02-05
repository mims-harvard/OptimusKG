from enum import Enum

import polars as pl


class Node(str, Enum):
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
    PROTEIN = "PRO"


class Edge(str, Enum):
    @staticmethod
    def format_label(src: Node, dst: Node) -> str:
        return f"{src.value}-{dst.value}"


class Relation(str, Enum):
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

    # Expression relations (anatomy-protein)
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

    # Drug-Protein role relations
    TARGET = "TARGET"
    ENZYME = "ENZYME"
    TRANSPORTER = "TRANSPORTER"
    CARRIER = "CARRIER"

    # Drug-Protein action relations
    ACTIVATOR = "ACTIVATOR"
    AGONIST = "AGONIST"
    ALLOSTERIC_ANTAGONIST = "ALLOSTERIC_ANTAGONIST"
    ANTAGONIST = "ANTAGONIST"
    BINDING_AGENT = "BINDING_AGENT"
    BLOCKER = "BLOCKER"
    DEGRADER = "DEGRADER"
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
    RELEASING_AGENT = "RELEASING_AGENT"
    STABILISER = "STABILISER"
    SUBSTRATE = "SUBSTRATE"


# Priority mapping for relation resolution (lower = higher priority)
# When multiple relations exist, the one with lowest priority number wins
RELATION_PRIORITY: dict[Relation, int] = {
    # Drug-Disease priorities
    Relation.OFF_LABEL_USE: 1,  # Most specific
    Relation.CONTRAINDICATION: 2,
    Relation.INDICATION: 3,
    # Drug-Drug priorities
    Relation.SYNERGISTIC_INTERACTION: 1,  # Specific interaction
    Relation.PARENT: 10,  # Generic hierarchy
    # Drug-Protein action types (all equal, most specific)
    Relation.ACTIVATOR: 1,
    Relation.AGONIST: 1,
    Relation.ALLOSTERIC_ANTAGONIST: 1,
    Relation.ANTAGONIST: 1,
    Relation.BINDING_AGENT: 1,
    Relation.BLOCKER: 1,
    Relation.DEGRADER: 1,
    Relation.INHIBITOR: 1,
    Relation.INVERSE_AGONIST: 1,
    Relation.MODULATOR: 1,
    Relation.NEGATIVE_ALLOSTERIC_MODULATOR: 1,
    Relation.NEGATIVE_MODULATOR: 1,
    Relation.OPENER: 1,
    Relation.PARTIAL_AGONIST: 1,
    Relation.POSITIVE_ALLOSTERIC_MODULATOR: 1,
    Relation.POSITIVE_MODULATOR: 1,
    Relation.RELEASING_AGENT: 1,
    Relation.STABILISER: 1,
    Relation.SUBSTRATE: 1,
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


def resolve_relation(relations: pl.Series) -> str:
    """Resolve multiple Relation values to the highest priority one.

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
