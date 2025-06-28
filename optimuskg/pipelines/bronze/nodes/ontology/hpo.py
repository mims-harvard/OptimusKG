import logging

import polars as pl
from kedro.pipeline import node
from lxml import etree

logger = logging.getLogger(__name__)


def run(
    human_phenotype_ontology: etree._ElementTree,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Process Human Phenotype Ontology to extract phenotypes, cross-references, and parent-child relationships.

    Returns:
        tuple: (phenotypes_df, xrefs_df, parents_df)
            - phenotypes_df: Contains HP terms with id, name, source
            - xrefs_df: Contains cross-references with hp_id, ontology, ontology_id
            - parents_df: Contains parent-child relationships with parent, child
    """
    root = human_phenotype_ontology.getroot()

    namespaces = {
        "owl": "http://www.w3.org/2002/07/owl#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "obo": "http://purl.obolibrary.org/obo/",
        "oboInOwl": "http://www.geneontology.org/formats/oboInOwl#",
    }

    phenotypes = []
    xrefs = []
    parents = []

    phenotypes_schema = {"id": pl.Utf8, "name": pl.Utf8, "source": pl.Utf8}
    xrefs_schema = {"hp_id": pl.Utf8, "ontology": pl.Utf8, "ontology_id": pl.Utf8}
    parents_schema = {"parent": pl.Utf8, "child": pl.Utf8}

    xpath_results: etree._XPathObject = root.xpath("//owl:Class", namespaces=namespaces)
    if not isinstance(xpath_results, list):
        logger.error("XPath results are not a list, returning empty DataFrames")
        return (
            pl.DataFrame(schema=phenotypes_schema),
            pl.DataFrame(schema=xrefs_schema),
            pl.DataFrame(schema=parents_schema),
        )

    for class_elem in xpath_results:
        if not isinstance(class_elem, etree._Element):
            logger.debug(f"Skipping non-element: {class_elem!r}")
            continue

        about_attr = class_elem.get(
            "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"
        )

        if about_attr and "HP_" in about_attr:
            hp_id = about_attr.split("/")[-1]

            # Extract name/label
            label_elem: etree._Element | None = class_elem.find(
                ".//rdfs:label", namespaces=namespaces
            )
            name: str | None = None
            if label_elem is not None:
                name = label_elem.text

            if hp_id and name:
                phenotypes.append(
                    {
                        "id": hp_id,
                        "name": name,
                        "source": "HPO",
                    }
                )

            # Extract cross-references
            xref_elems = class_elem.findall(
                ".//oboInOwl:hasDbXref", namespaces=namespaces
            )
            for xref_elem in xref_elems:
                if xref_elem.text:
                    xref_text = xref_elem.text.strip()
                    if ":" in xref_text:
                        # Split on the first colon to separate ontology from ID
                        ontology, ontology_id = xref_text.split(":", 1)
                        xrefs.append(
                            {
                                "hp_id": hp_id,
                                "ontology": ontology,
                                "ontology_id": ontology_id,
                            }
                        )

            # Extract parent-child relationships (is_a relationships)
            subclass_elems = class_elem.findall(
                ".//rdfs:subClassOf", namespaces=namespaces
            )
            for subclass_elem in subclass_elems:
                # Check if it's a direct resource reference (not a restriction)
                resource_attr = subclass_elem.get(
                    "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource"
                )
                if resource_attr and "HP_" in resource_attr:
                    parent_id = resource_attr.split("/")[-1]
                    parents.append(
                        {
                            "parent": parent_id,
                            "child": hp_id,
                        }
                    )

    # Create DataFrames
    phenotypes_df = pl.DataFrame(phenotypes, schema=phenotypes_schema)
    phenotypes_df = phenotypes_df.sort("id")

    xrefs_df = pl.DataFrame(xrefs, schema=xrefs_schema)
    xrefs_df = xrefs_df.sort(["hp_id", "ontology"])

    parents_df = pl.DataFrame(parents, schema=parents_schema)
    parents_df = parents_df.sort(["parent", "child"])

    logger.debug(
        f"Extracted {len(phenotypes_df)} phenotypes, {len(xrefs_df)} cross-references, and {len(parents_df)} parent-child relationships"
    )
    logger.debug(
        f"Cross-references found for {len(xrefs_df.get_column('ontology').unique())} different ontologies"
    )

    return phenotypes_df, xrefs_df, parents_df


hpo_node = node(
    run,
    inputs={
        "human_phenotype_ontology": "landing.ontology.hpo",
    },
    outputs=[
        "ontology.phenotypes",
        "ontology.phenotypes_xrefs",
        "ontology.phenotypes_parents",
    ],
    name="phenotypes",
    tags=["bronze"],
)
