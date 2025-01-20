import logging

import polars as pl
from bs4 import BeautifulSoup
from kedro.pipeline import node

log = logging.getLogger(__name__)


def process_drug_drug(
    full_database: str,
) -> pl.DataFrame:
    soup = BeautifulSoup(full_database, "xml")
    interactions = []
    for drug in soup.find_all("drug"):
        drug_id = drug.find("drugbank-id").text
        for interaction in drug.find_all("drug-interaction"):
            interacting_id = interaction.find("drugbank-id").text
            interactions.append((drug_id, interacting_id))
    return pl.DataFrame(interactions, schema=["drug1", "drug2"], orient="row")


drug_drug_node = node(
    process_drug_drug,
    inputs={
        "full_database": "landing.drugbank.full_database",
    },
    outputs="drugbank.drug_drug",
    name="drug_drug",
)
