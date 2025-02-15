#!/bin/bash

# API and URL configurations
BIOONTOLOGY_API_KEY="168d52c1-bd36-4866-aa99-0aa8eb06f295" # TODO: Parameterize this, but for now is safe to use this key (it's public)
BIOONTOLOGY_DOMAIN="data.bioontology.org/ontologies"
OBOLIBRARY_DOMAIN="purl.obolibrary.org/obo"

# Ontology versions (submissions)
BIOLINK_VERSION="68"
DOID_VERSION="656"
GO_PLUS_VERSION="701"
UBERON_VERSION="348"

# Base URLs and directories
BASE_DIR="./data/landing/ontologies"


download_ontology() {
    local name="$1"
    local url="$2"
    local output_file="$3"

    if [ ! -f "$output_file" ]; then
        echo "Downloading $name ontology..."
        curl -L "$url" -o "$output_file"
    else
        echo "Skipping $name ontology - already exists"
    fi
}

# Biolink ontology
download_ontology "Biolink" \
    "https://${BIOONTOLOGY_DOMAIN}/BIOLINK/submissions/${BIOLINK_VERSION}/download?apikey=${BIOONTOLOGY_API_KEY}" \
    "${BASE_DIR}/biolink.owl"

# Disease ontology
download_ontology "Disease" \
    "https://${BIOONTOLOGY_DOMAIN}/DOID/submissions/${DOID_VERSION}/download?apikey=${BIOONTOLOGY_API_KEY}" \
    "${BASE_DIR}/disease_ontology.owl"

# Gene ontology
download_ontology "Gene" \
    "https://${BIOONTOLOGY_DOMAIN}/GO-PLUS/submissions/${GO_PLUS_VERSION}/download?apikey=${BIOONTOLOGY_API_KEY}" \
    "${BASE_DIR}/gene_ontology.owl"

# Human phenotype ontology
download_ontology "Human Phenotype" \
    "http://${OBOLIBRARY_DOMAIN}/hp.owl" \
    "${BASE_DIR}/human_phenotype_ontology.owl"

# Mondo ontology
download_ontology "Mondo" \
    "http://${OBOLIBRARY_DOMAIN}/mondo.owl" \
    "${BASE_DIR}/mondo.owl"

# Orphanet ontology
download_ontology "Orphanet" \
    "http://${OBOLIBRARY_DOMAIN}/ordo.owl" \
    "${BASE_DIR}/orphanet.owl"

# Uber anatomy ontology
download_ontology "Uber Anatomy" \
    "https://${BIOONTOLOGY_DOMAIN}/UBERON/submissions/${UBERON_VERSION}/download?apikey=${BIOONTOLOGY_API_KEY}" \
    "${BASE_DIR}/uber_anatomy_ontology.owl"
