import polars as pl
from kedro.pipeline import node
from lxml import etree


def process_drug_drug(full_database: etree._ElementTree) -> pl.DataFrame:
    """Extract drug-drug interactions from DrugBank XML database.

    Args:
        full_database: XML tree containing the DrugBank database

    Returns:
        DataFrame containing drug-drug interaction pairs
    """
    root = full_database.getroot()

    # Define namespace if present in the XML
    nsmap = root.nsmap

    # Use default namespace, otherwise empty string
    ns = f"{{{nsmap[None]}}}" if None in nsmap else ""

    interactions = []

    for drug in root.findall(f".//{ns}drug"):
        head_drug_id = drug.find(f"{ns}drugbank-id").text  # type: ignore

        # Find all drug interactions
        for interaction in drug.findall(f".//{ns}drug-interaction"):
            tail_drug_id = interaction.find(f"{ns}drugbank-id").text  # type: ignore
            interaction_description = interaction.find(f"{ns}description").text  # type: ignore
            interactions.append((tail_drug_id, head_drug_id, interaction_description))

    return pl.DataFrame(
        interactions, schema=["tail_drug", "head_drug", "description"], orient="row"
    )


drug_drug_node = node(
    process_drug_drug,
    inputs={
        "full_database": "landing.drugbank.full_database",
    },
    outputs="drugbank.drug_drug",
    name="drug_drug",
)
