import polars as pl
from kedro.pipeline import node
from lxml import etree


def run(full_database: etree._ElementTree) -> pl.DataFrame:
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
        head_drug_id_text = drug.find(f"{ns}drugbank-id").text  # type: ignore
        head_drug_id = f"DRUGBANK:{head_drug_id_text}"

        # Find all drug interactions
        for interaction in drug.findall(f".//{ns}drug-interaction"):
            tail_drug_id_text = interaction.find(f"{ns}drugbank-id").text  # type: ignore
            tail_drug_id = f"DRUGBANK:{tail_drug_id_text}"
            interaction_description = interaction.find(f"{ns}description").text  # type: ignore

            id1, id2 = sorted([tail_drug_id, head_drug_id])
            interactions.append((id1, id2, interaction_description))

    return pl.DataFrame(
        interactions,
        schema=["tail_drug_id", "head_drug_id", "description"],
        orient="row",
    )


drug_drug_node = node(
    run,
    inputs={
        "full_database": "landing.drugbank.full_database",
    },
    outputs="drug_drug",
    name="drug_drug",
    tags=["bronze"],
)
