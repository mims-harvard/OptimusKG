#!/bin/bash

# DrugBank base URL and directory
DRUGBANK_BASE_URL="https://www.genenames.org/cgi-bin/download/custom?col=md_eg_id&col=md_prot_id&status=Approved&hgnc_dbtag=on&order_by=gd_app_sym_sort&format=text&submit=submit"
DRUGBANK_DIR="./data/landing/drugbank"

# Gene names base URL and directory
GENE_NAMES_BASE_URL="https://www.genenames.org/cgi-bin/download/custom?col=gd_app_sym&col=gd_app_name&col=gd_pub_acc_ids&col=gd_pub_refseq_ids&col=gd_pub_eg_id&col=md_eg_id&col=md_prot_id&col=md_mim_id&status=Approved&hgnc_dbtag=on&order_by=gd_app_sym_sort&format=text&submit=submit"
GENE_NAMES_DIR="./data/landing/gene_names"

# Base URLs and directories
BASE_URL="http://ftp.ebi.ac.uk/pub/databases/opentargets/platform/24.09/output/etl"
BASE_DIR="./data/landing/opentargets"

# Subdirectories
EVIDENCE_SUBDIR="./evidence"
TARGETS_DIR="./targets"
MOLECULE_DIR="./molecule"
DISEASES_DIR="./diseases"
DISEASES_TO_PHENOTYPE="./disease_to_phenotype"

# Evidence source IDs
SOURCE_IDS=(
    "cancer_gene_census"
    "chembl"
    "clingen"
    "crispr" 
    "crispr_screen"
    "expression_atlas"
    "gene_burden"
    "gene2phenotype"
    "genomics_england"
    "intogen"
    "progeny"
    "reactome"
    "slapenrich"
    "sysbio"
    "uniprot_literature"
    "orphanet"
)

download_files() {
    local source_name="$1"
    local download_dir="$2"
    local source_url="$3"
    local file_extension="$4"

    mkdir -p "$download_dir"
    cd "$download_dir" || exit

    files=$(curl -s "$source_url" | grep -oP "(?<=href=\")[^\"]*\.$file_extension(?=\")")

    for file in $files; do
        if [ ! -f "$file" ]; then
            echo "Downloading $file from $source_name..."
            curl -O "$source_url$file"
        else
            echo "Skipping $file - already exists"
        fi
    done

    cd - > /dev/null
}

# Download opentargets data

for source in "${SOURCE_IDS[@]}"; do
    download_files "$source" "$BASE_DIR/$EVIDENCE_SUBDIR/$source" "$BASE_URL/parquet/evidence/sourceId=$source/" "parquet"
done

download_files "targets" "$BASE_DIR/$TARGETS_DIR" "$BASE_URL/json/targets/" "json"

download_files "molecule" "$BASE_DIR/$MOLECULE_DIR" "$BASE_URL/json/molecule/" "json"

download_files "diseases" "$BASE_DIR/$DISEASES_DIR" "$BASE_URL/json/diseases/" "json"

download_files "diseaseToPhenotype" "$BASE_DIR/$DISEASES_TO_PHENOTYPE" "$BASE_URL/json/diseaseToPhenotype/" "json"

## Download mondo_efo_mappings data

mkdir -p "./data/landing/opentargets/mondo_efo_mappings.tsv"
echo "Downloading mondo_efo_mappings data..."
curl -o "./data/landing/opentargets/mondo_efo_mappings.tsv" "https://raw.githubusercontent.com/EBISPOT/efo/refs/heads/master/src/ontology/components/mondo_efo_mappings.tsv"

## Download drug_mappings data

mkdir -p "./data/landing/opentargets/drug_mappings.tsv"
echo "Downloading drug_mappings data..."
curl -o "./data/landing/opentargets/drug_mappings.tsv" "https://raw.githubusercontent.com/iit-Demokritos/drug_id_mapping/refs/heads/main/drug-mappings.tsv"


# Download gene names data
mkdir -p "$GENE_NAMES_DIR"
echo "Downloading gene names data..."
curl -o "$GENE_NAMES_DIR/gene_names.tsv" "$GENE_NAMES_BASE_URL"

# Download drugbank gene map data
mkdir -p "$DRUGBANK_DIR"
echo "Downloading drugbank gene map data..."
curl -o "$DRUGBANK_DIR/drugbank_gene_map.tsv" "$DRUGBANK_BASE_URL"

# Download ontologies
./scripts/download_landing_ontologies.sh