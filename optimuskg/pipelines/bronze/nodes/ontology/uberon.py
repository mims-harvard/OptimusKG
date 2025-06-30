import logging

import polars as pl
from kedro.pipeline import node
from lxml import etree
from lxml.etree import _Element

logger = logging.getLogger(__name__)


def run(
    uberon_ontology: etree._ElementTree,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Process Uberon ontology to extract terms and 'is_a' relationships.

    This function parses an Uberon OWL file, extracts relevant information for each term
    (ID, name, definition), and identifies hierarchical ('is_a') relationships
    between terms. It performs cleaning and filtering similar to the
    original script, like removing obsolete terms and ensuring relationship targets are
    within the Uberon ontology.

    Args:
        uberon_ontology: An lxml ElementTree object representing the Uberon OWL file.

    Returns:
        A tuple containing two Polars DataFrames:
        - uberon_terms: DataFrame with 'id', 'name', and 'def' of Uberon terms.
        - uberon_relations: DataFrame with 'id', 'relation_type', and 'relation_id' for 'is_a' relationships.
    """
    root = uberon_ontology.getroot()

    namespaces = {
        "owl": "http://www.w3.org/2002/07/owl#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "obo": "http://purl.obolibrary.org/obo/",
        "oboInOwl": "http://www.geneontology.org/formats/oboInOwl#",
    }

    terms = []
    relations = []

    terms_schema = {"id": pl.Utf8, "name": pl.Utf8, "def": pl.Utf8}
    relations_schema = {
        "id": pl.Utf8,
        "relation_type": pl.Utf8,
        "relation_id": pl.Utf8,
    }

    class_elements_raw = root.xpath("//owl:Class", namespaces=namespaces)
    if not isinstance(class_elements_raw, list):
        class_elements_raw = []

    for class_elem_any in class_elements_raw:
        if not isinstance(class_elem_any, _Element):
            continue
        class_elem: _Element = class_elem_any

        about_attr = class_elem.get(f"{{{namespaces['rdf']}}}about")
        if not about_attr or "UBERON" not in about_attr:
            continue

        term_id = about_attr.split("/")[-1]

        # Skip obsolete terms
        if class_elem.find("owl:deprecated", namespaces=namespaces) is not None:
            continue

        label_elem = class_elem.find("rdfs:label", namespaces=namespaces)
        name = label_elem.text if label_elem is not None else None

        def_elem = class_elem.find("obo:IAO_0000115", namespaces=namespaces)
        definition = def_elem.text if def_elem is not None else None

        has_is_a = False
        for sub_class_elem_any in class_elem.findall(
            "rdfs:subClassOf", namespaces=namespaces
        ):
            if not isinstance(sub_class_elem_any, _Element):
                continue
            sub_class_elem: _Element = sub_class_elem_any

            # Direct 'is_a' relationships
            resource_attr = sub_class_elem.get(f"{{{namespaces['rdf']}}}resource")
            if (
                resource_attr
                and "UBERON" in resource_attr
                and not resource_attr.startswith("_:")
            ):
                is_a_id = resource_attr.split("/")[-1]
                relations.append(
                    {
                        "id": term_id,
                        "relation_type": "is_a",
                        "relation_id": is_a_id,
                    }
                )
                has_is_a = True

        if has_is_a and name:
            terms.append({"id": term_id, "name": name, "def": definition})

    # Create DataFrames
    uberon_terms = pl.DataFrame(terms, schema=terms_schema)
    uberon_relations = pl.DataFrame(relations, schema=relations_schema)

    uberon_terms = uberon_terms.sort("id")
    uberon_relations = uberon_relations.sort(["id", "relation_id"])

    logger.debug(f"Extracted {len(uberon_terms)} Uberon terms.")
    logger.debug(f"Extracted {len(uberon_relations)} 'is_a' relationships.")

    return uberon_terms, uberon_relations


uberon_node = node(
    run,
    inputs={
        "uberon_ontology": "landing.ontology.uberon",
    },
    outputs=[
        "ontology.uberon_terms",
        "ontology.uberon_relations",
    ],
    name="uberon",
    tags=["bronze"],
)
