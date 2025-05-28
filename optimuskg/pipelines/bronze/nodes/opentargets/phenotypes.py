import polars as pl
from kedro.pipeline import node
from lxml import etree


def process_phenotypes(
    human_phenotype_ontology: pl.DataFrame,
) -> pl.DataFrame:
    root = human_phenotype_ontology.getroot()

    namespaces = {
        "owl": "http://www.w3.org/2002/07/owl#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "obo": "http://purl.obolibrary.org/obo/",
    }

    phenotypes = []

    # Find all owl:Class elements that represent HP terms
    for class_elem in root.xpath("//owl:Class", namespaces=namespaces):
        about_attr = class_elem.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")

        if about_attr and "HP_" in about_attr:
            hp_id = about_attr.split("/")[-1]

            # Find the label (name) for this term
            label_elem = class_elem.find(".//rdfs:label", namespaces=namespaces)
            name = label_elem.text if label_elem is not None else None

            if hp_id and name:
                phenotypes.append(
                    {
                        "id": hp_id,
                        "node_name": name,
                        # 'node_source': 'HPO'
                    }
                )

    df = pl.DataFrame(phenotypes)
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
