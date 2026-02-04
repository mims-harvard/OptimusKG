from enum import Enum


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
