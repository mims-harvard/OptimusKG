import logging
import os
import re
from pathlib import Path

import requests

from cli import paths

logger = logging.getLogger(__name__)


def _mkdir(directory: Path) -> None:
    """Create directory if it doesn't exist."""
    directory.mkdir(parents=True, exist_ok=True)


def _download_file(
    url: str, output_path: Path, logger_description: str, chunk_size: int = 8192
) -> None:
    """Download a file if it doesn't exist."""
    if not output_path.exists():
        logger.info(f"Downloading {logger_description}...")
        response = requests.get(url, stream=True, allow_redirects=True)
        response.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)
        logger.info(f"Downloaded {logger_description}")
    else:
        logger.info(f"Skipping {logger_description} - already exists")


def _ls_from_url(url: str, extension: str) -> list[str]:
    """Get a list of files with the given extension from a URL."""
    response = requests.get(url)
    response.raise_for_status()

    pattern = f'(?<=href=")[^"]*\\.{extension}(?=")'
    files = re.findall(pattern, response.text)
    return files


def _download_files(
    output_dir: Path,
    source_url: str,
    file_extension: str,
    chunk_size: int = 8192,
):
    """Download all files with the given extension from a URL."""
    _mkdir(output_dir)

    try:
        for file in _ls_from_url(source_url, file_extension):
            _download_file(
                url=f"{source_url}{file}",
                output_path=output_dir / os.path.basename(file),
                logger_description=file,
                chunk_size=chunk_size,
            )
    except Exception as e:
        logger.error(f"Error downloading files from {source_url}: {e}")
        raise


def download_opentargets(
    evidence_source_ids: list[str] = [
        "cancer_gene_census",
        "chembl",
        "clingen",
        "crispr",
        "crispr_screen",
        "expression_atlas",
        "gene_burden",
        "gene2phenotype",
        "genomics_england",
        "intogen",
        "progeny",
        "reactome",
        "slapenrich",
        "sysbio",
        "uniprot_literature",
        "orphanet",
    ],
    base_url: str = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/24.09/output/etl",
) -> None:
    """Download all files with the given extension from a URL."""

    # Download evidence files
    for source in evidence_source_ids:
        _download_files(
            output_dir=paths.DATA_LANDING_OPENTARGETS_DIR / "evidence" / source,
            source_url=f"{base_url}/parquet/evidence/sourceId={source}/",
            file_extension="parquet",
        )

    # Download the remaining files
    remaining_files = {
        "targets": {
            "output_dir": paths.DATA_LANDING_OPENTARGETS_DIR / "targets",
            "file_extension": "json",
        },
        "molecule": {
            "output_dir": paths.DATA_LANDING_OPENTARGETS_DIR / "molecule",
            "file_extension": "json",
        },
        "diseases": {
            "output_dir": paths.DATA_LANDING_OPENTARGETS_DIR / "diseases",
            "file_extension": "json",
        },
        "diseaseToPhenotype": {
            "output_dir": paths.DATA_LANDING_OPENTARGETS_DIR / "disease_to_phenotype",
            "file_extension": "json",
        },
    }

    for name, config in remaining_files.items():
        _download_files(
            output_dir=config["output_dir"],
            source_url=f"{base_url}/{config['file_extension']}/{name}/",
            file_extension=config["file_extension"],
        )

    # Download MONDO-EFO mappings
    _download_file(
        url="https://raw.githubusercontent.com/EBISPOT/efo/refs/heads/master/src/ontology/components/mondo_efo_mappings.tsv",
        output_path=paths.DATA_LANDING_OPENTARGETS_DIR / "mondo_efo_mappings.tsv",
        logger_description="MONDO-EFO mappings",
    )

    # Download drug mappings
    _download_file(
        url="https://raw.githubusercontent.com/iit-Demokritos/drug_id_mapping/refs/heads/main/drug-mappings.tsv",
        output_path=paths.DATA_LANDING_OPENTARGETS_DIR / "drug_mappings.tsv",
        logger_description="Drug ID mappings",
    )


def download_ontologies(  # noqa: PLR0913
    bioontology_api_key: str = "168d52c1-bd36-4866-aa99-0aa8eb06f295",  # NOTE: is safe to use this key as default value (it's public)
    biolink_version: str = "68",
    doid_version: str = "656",
    go_plus_version: str = "701",
    uberon_version: str = "348",
    ordo_version: str = "30",
    chunk_size: int = 8192,
) -> None:
    """Download all ontology files to the landing zone."""
    _mkdir(paths.DATA_LANDING_ONTOLOGY_DIR)

    ontologies = {
        "Biolink": {
            "url": f"https://data.bioontology.org/ontologies/BIOLINK/submissions/{biolink_version}/download?apikey={bioontology_api_key}",
            "output_path": paths.DATA_LANDING_ONTOLOGY_DIR / "biolink.owl",
        },
        "Disease": {
            "url": f"https://data.bioontology.org/ontologies/DOID/submissions/{doid_version}/download?apikey={bioontology_api_key}",
            "output_path": paths.DATA_LANDING_ONTOLOGY_DIR / "disease_ontology.owl",
        },
        "Gene": {
            "url": f"https://data.bioontology.org/ontologies/GO-PLUS/submissions/{go_plus_version}/download?apikey={bioontology_api_key}",
            "output_path": paths.DATA_LANDING_ONTOLOGY_DIR / "gene_ontology.owl",
        },
        "Uber Anatomy": {
            "url": f"https://data.bioontology.org/ontologies/UBERON/submissions/{uberon_version}/download?apikey={bioontology_api_key}",
            "output_path": paths.DATA_LANDING_ONTOLOGY_DIR
            / "uber_anatomy_ontology.owl",
        },
        "Human Phenotype": {
            "url": "http://purl.obolibrary.org/obo/hp.owl",
            "output_path": paths.DATA_LANDING_ONTOLOGY_DIR
            / "human_phenotype_ontology.owl",
        },
        "Mondo": {
            "url": "http://purl.obolibrary.org/obo/mondo.owl",
            "output_path": paths.DATA_LANDING_ONTOLOGY_DIR / "mondo.owl",
        },
        "Orphanet": {
            "url": f"https://data.bioontology.org/ontologies/ORDO/submissions/{ordo_version}/download?apikey={bioontology_api_key}",
            "output_path": paths.DATA_LANDING_ONTOLOGY_DIR / "orphanet.owl",
        },
    }

    for name, config in ontologies.items():
        _download_file(
            url=config["url"],
            output_path=config["output_path"],
            logger_description=name,
            chunk_size=chunk_size,
        )


def download_drugbank_files(
    chunk_size: int = 8192,
) -> None:
    """Download all DrugBank files to the landing zone."""
    _mkdir(paths.DATA_LANDING_DRUGBANK_DIR)

    _download_file(
        url="https://www.genenames.org/cgi-bin/download/custom?col=md_eg_id&col=md_prot_id&status=Approved&hgnc_dbtag=on&order_by=gd_app_sym_sort&format=text&submit=submit",
        output_path=paths.DATA_LANDING_DRUGBANK_DIR / "drugbank_gene_map.tsv",
        logger_description="DrugBank gene mappings",
        chunk_size=chunk_size,
    )


def download_gene_names_files(
    chunk_size: int = 8192,
) -> None:
    """Download all gene names files to the landing zone."""
    _mkdir(paths.DATA_LANDING_GENE_NAMES_DIR)

    _download_file(
        url="https://www.genenames.org/cgi-bin/download/custom?col=gd_app_sym&col=gd_app_name&col=gd_pub_acc_ids&col=gd_pub_refseq_ids&col=gd_pub_eg_id&col=md_eg_id&col=md_prot_id&col=md_mim_id&status=Approved&hgnc_dbtag=on&order_by=gd_app_sym_sort&format=text&submit=submit",
        output_path=paths.DATA_LANDING_GENE_NAMES_DIR / "gene_names.tsv",
        logger_description="Gene names",
        chunk_size=chunk_size,
    )
