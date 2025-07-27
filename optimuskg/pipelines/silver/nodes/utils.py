import logging

import requests
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class GeneMetadata(BaseModel):
    ncbi_id: str = Field(..., description="NCBI Gene ID", nullable=False)
    symbol: str = Field(..., description="Gene symbol", nullable=False)
    name: str = Field(..., description="Gene name", nullable=False)
    tax_id: str = Field(
        ..., description="NCBI Taxonomy ID for the organism", nullable=False
    )
    tax_name: str = Field(
        ..., description="Taxonomic name of the organism", nullable=False
    )
    common_name: str = Field(
        ..., description="Common name of the organism", nullable=False
    )
    type: str = Field(
        ..., description="GeneType values match Entrez Gene", nullable=False
    )
    orientation: str = Field(..., description="The gene orientation", nullable=False)
    chromosomes: list[str] = Field(..., description="The chromosomes", nullable=False)
    swiss_prot_accessions: list[str] = Field(
        ..., description="The Swiss-Prot accessions", nullable=False
    )
    ensembl_gene_ids: list[str] = Field(
        ..., description="The Ensembl gene IDs", nullable=False
    )
    omim_ids: list[str] = Field(..., description="The OMIM IDs", nullable=False)
    synonyms: list[str] = Field(..., description="The synonyms", nullable=False)
    transcript_count: int = Field(
        ..., description="Number of transcripts encoded by the gene", nullable=False
    )
    protein_count: int = Field(
        ..., description="Number of proteins encoded by the gene", nullable=False
    )
    summary: str = Field(
        ...,
        description="Gene summary text itself that describes the gene",
        nullable=False,
    )


def get_gene_metadata_by_symbol(symbol: str) -> GeneMetadata:
    response = requests.get(
        f"https://api.ncbi.nlm.nih.gov/datasets/v2/gene/symbol/{symbol}/taxon/9606",
        headers={"accept": "application/json"},
    )
    response.raise_for_status()
    gene_report = response.json()["reports"][0][0]["gene"]
    gene_metadata = GeneMetadata(
        ncbi_id=gene_report["gene_id"],
        symbol=gene_report["symbol"],
        name=gene_report["description"],
        tax_id=gene_report["tax_id"],
        tax_name=gene_report["tax_name"],
        common_name=gene_report["common_name"],
        type=gene_report["type"],
        orientation=gene_report["orientation"],
        chromosomes=gene_report["chromosomes"],
        swiss_prot_accessions=gene_report["swiss_prot_accessions"],
        ensembl_gene_ids=gene_report["ensembl_gene_ids"],
        omim_ids=gene_report["omim_ids"],
        synonyms=gene_report["synonyms"],
        transcript_count=gene_report["transcript_count"],
        protein_count=gene_report["protein_count"],
        summary=gene_report["summary"][0]["description"],
    )
    logger.info(f"Gene metadata for {symbol}: {gene_metadata}")
    return gene_metadata
