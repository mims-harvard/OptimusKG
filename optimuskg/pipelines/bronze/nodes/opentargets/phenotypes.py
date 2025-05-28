import logging

import polars as pl
from kedro.pipeline import node
from lxml import etree

logger = logging.getLogger(__name__)


def process_phenotypes(
    human_phenotype_ontology: etree._ElementTree,
) -> pl.DataFrame:
    root = human_phenotype_ontology.getroot()

    namespaces = {
        "owl": "http://www.w3.org/2002/07/owl#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "obo": "http://purl.obolibrary.org/obo/",
    }

    phenotypes = []
    df_schema = {"id": pl.Utf8, "name": pl.Utf8, "source": pl.Utf8}

    xpath_results: etree._XPathObject = root.xpath("//owl:Class", namespaces=namespaces)
    if not isinstance(xpath_results, list):
        logger.error("XPath results are not a list, returning empty DataFrame")
        return pl.DataFrame(schema=df_schema)

    for class_elem in xpath_results:
        if not isinstance(class_elem, etree._Element):
            logger.debug(f"Skipping non-element: {class_elem!r}")
            continue

        about_attr = class_elem.get(
            "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"
        )

        if about_attr and "HP_" in about_attr:
            hp_id = about_attr.split("/")[-1]

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

    df = pl.DataFrame(phenotypes, schema=df_schema)
    df = df.sort("id")

    return df


phenotypes_node = node(
    process_phenotypes,
    inputs={
        "human_phenotype_ontology": "landing.ontology.human_phenotype",
    },
    outputs="opentargets.phenotypes",
    name="phenotypes",
    tags=["bronze"],
)
