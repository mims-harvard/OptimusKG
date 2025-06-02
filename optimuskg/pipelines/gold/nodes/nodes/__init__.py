from .anatomical_entity import (
    ANATOMICAL_ENTITY_NODE_MAPPING_CONFIG,
    anatomical_entity_node,
)
from .biological_process import (
    BIOLOGICAL_PROCESS_NODE_MAPPING_CONFIG,
    biological_process_node,
)
from .cellular_component import (
    CELLULAR_COMPONENT_NODE_MAPPING_CONFIG,
    cellular_component_node,
)
from .disease import DISEASE_NODE_MAPPING_CONFIG, disease_node
from .drug import DRUG_NODE_MAPPING_CONFIG, drug_node
from .environmental_exposure import (
    ENVIRONMENTAL_EXPOSURE_NODE_MAPPING_CONFIG,
    environmental_exposure_node,
)
from .gene import GENE_NODE_MAPPING_CONFIG, gene_node
from .molecular_function import (
    MOLECULAR_FUNCTION_NODE_MAPPING_CONFIG,
    molecular_function_node,
)
from .pathway import PATHWAY_NODE_MAPPING_CONFIG, pathway_node
from .phenotype import PHENOTYPE_NODE_MAPPING_CONFIG, phenotype_node

NODE_MAPPING_CONFIGS = [
    ANATOMICAL_ENTITY_NODE_MAPPING_CONFIG,
    BIOLOGICAL_PROCESS_NODE_MAPPING_CONFIG,
    CELLULAR_COMPONENT_NODE_MAPPING_CONFIG,
    DISEASE_NODE_MAPPING_CONFIG,
    DRUG_NODE_MAPPING_CONFIG,
    ENVIRONMENTAL_EXPOSURE_NODE_MAPPING_CONFIG,
    GENE_NODE_MAPPING_CONFIG,
    MOLECULAR_FUNCTION_NODE_MAPPING_CONFIG,
    PATHWAY_NODE_MAPPING_CONFIG,
    PHENOTYPE_NODE_MAPPING_CONFIG,
]

__all__ = [
    "NODE_MAPPING_CONFIGS",
    "anatomical_entity_node",
    "biological_process_node",
    "cellular_component_node",
    "disease_node",
    "drug_node",
    "environmental_exposure_node",
    "gene_node",
    "molecular_function_node",
    "pathway_node",
    "phenotype_node",
]
