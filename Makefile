.DEFAULT_GOAL := help

.PHONY: help
help: ##@ (Default) List available commands with their descriptions
	@printf "\nUsage: make <command>\n"
	@grep -F -h "##@" $(MAKEFILE_LIST) | grep -F -v grep -F | sed -e 's/\\$$//' | awk 'BEGIN {FS = ":*[[:space:]]*##@[[:space:]]*"}; \
	{ \
		if($$2 == "") \
			pass; \
		else if($$0 ~ /^#/) \
			printf "%s", $$2; \
		else if($$1 == "") \
			printf "     %-20s%s", "", $$2; \
		else \
			printf "    \033[34m%-20s\033[0m %s\n", $$1, $$2; \
	}'

.PHONY: .uv
.uv: ##@ Check that uv is installed
	@uv --version || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

.PHONY: .pre-commit
.pre-commit: ##@ Setup pre-commit hooks for the project
	@uv run pre-commit -V || (echo 'Please install pre-commit: https://pre-commit.com/' && exit 1)
	@uv run pre-commit install

.PHONY: format
format: ##@ Format code
	@uv run ruff check --fix --exit-non-zero-on-fix optimuskg
	@uv run ruff format optimuskg

.PHONY: lint
lint: ##@ Lint code
	@uv run ruff check optimuskg
	@uv run ruff format --check optimuskg

.PHONY: mypy
mypy: ##@ Run mypy
	@uv run mypy --install-types --config-file pyproject.toml --non-interactive --package optimuskg
	@uv run mypy --config-file pyproject.toml tests

.PHONY: pytest
pytest: ##@ Run tests
	@uv run pytest -W ignore --no-cov-on-fail --log-cli-level=INFO

.PHONY: bandit
bandit: ##@ Run bandit
	@uv tool run bandit -v -c pyproject.toml -r optimuskg/* tests/*

.PHONY: interrogate
interrogate: ##@ Run interrogate
	@uv tool run interrogate --config pyproject.toml

.PHONY: clean
clean: ##@ Clean up the project
	@rm -rf `find . -name __pycache__`
	@rm -f `find . -type f -name '*.py[co]'`
	@rm -f `find . -type f -name '*~'`
	@rm -f `find . -type f -name '.*~'`
	@rm -f `find . -type f -name '*.log'`
	@rm -rf `find . -type d -name '.ipynb_checkpoints'`
	@rm -rf `find . -type d -name '*.egg-info'`
	@rm -rf .cache
	@rm -rf dist
	@rm -rf .mypy_cache
	@rm -rf .venv
	@rm -rf .pytest_cache
	@rm -rf .ruff_cache
	@rm -rf htmlcov
	@rm -f .coverage*
	@rm -rf target

.PHONY: rm-data
rm-data: ##@ Remove the data directory
	@find data/ -type f -not -name ".gitkeep" -delete
	@find data/ -type d -empty -not -name ".gitkeep" -delete

.PHONY: neo4j
neo4j: ##@ Run the Neo4j container
	@docker compose up -d 

.PHONY: neo4j-import-data
neo4j-import-data: ##@ Import data into Neo4j    
	@docker run --interactive --tty --rm --publish=7474:7474 --publish=7687:7687 \
	    --volume=./data/neo4j/data:/data \
		--volume=./data/neo4j/import:/import \
		--volume=./data/export:/export \
		neo4j:5.26.2-community-bullseye \
		neo4j-admin database import full neo4j \
		--skip-duplicate-nodes \
		--bad-tolerance=1000000 \
		--verbose \
		--delimiter='\t' \
		--array-delimiter="|" \
		--quote='@' \
		--overwrite-destination=true \
		--nodes="/import/Disease-header.csv,/import/Disease-part.*" \
		--nodes="/import/Phenotype-header.csv,/import/Phenotype-part.*" \
		--nodes="/import/Drug-header.csv,/import/Drug-part.*" \
        --nodes="/import/Gene-header.csv,/import/Gene-part.*" \
        --nodes="/import/AnatomicalEntity-header.csv,/import/AnatomicalEntity-part.*" \
		--nodes="/import/EnvironmentalExposure-header.csv,/import/EnvironmentalExposure-part.*" \
        --relationships="/import/Anatomy_protein_absent-header.csv,/import/Anatomy_protein_absent-part.*" \
        --relationships="/import/Anatomy_protein_present-header.csv,/import/Anatomy_protein_present-part.*" \
        --relationships="/import/Exposure_exposure-header.csv,/import/Exposure_exposure-part.*" \
        --relationships="/import/Exposure_protein-header.csv,/import/Exposure_protein-part.*" \
		--relationships="/import/Disease_protein_positive-header.csv,/import/Disease_protein_positive-part.*" \
		--relationships="/import/Disease_protein_negative-header.csv,/import/Disease_protein_negative-part.*" \
		--relationships="/import/Drug_protein-header.csv,/import/Drug_protein-part.*" \
		--relationships="/import/Drug_drug-header.csv,/import/Drug_drug-part.*" \
		--relationships="/import/Indication-header.csv,/import/Indication-part.*" \
		--relationships="/import/Phenotype_protein-header.csv,/import/Phenotype_protein-part.*" \
		--relationships="/import/Strong_clinical_evidence-header.csv,/import/Strong_clinical_evidence-part.*" \
		--relationships="/import/Weak_clinical_evidence-header.csv,/import/Weak_clinical_evidence-part.*" \

.PHONY: neo4j-export-all
neo4j-export-all: ##@ Export Neo4j database to JSONL format
	@echo "Exporting Neo4j database to JSONL format..."
	@mkdir -p data/export
	@docker compose exec neo4j \
		cypher-shell --non-interactive \
		"CALL apoc.export.json.all('/var/lib/neo4j/export/optimuskg.pgjsonl', {jsonFormat: 'JSON_LINES', useTypes: true, stream: true})" && \
		echo "Database exported successfully to data/export/optimuskg.pgjsonl" || \
		echo "Export failed. Make sure Neo4j container is running with 'make neo4j' and APOC plugin is installed"

.PHONY: neo4j-export-query
neo4j-export-query: ##@ Export specific Neo4j query results to JSONL format
	@echo "Exporting specific query results to JSONL format..."
	@mkdir -p data/export
	@if [ -z "$$CYPHER_QUERY" ]; then echo "Error: Please set CYPHER_QUERY environment variable"; exit 1; fi; \
	export_filename=$$(echo "$$CYPHER_QUERY" | tr ' ' '_' | tr -cd '[:alnum:]_' | cut -c1-30); \
	docker compose exec neo4j \
		cypher-shell --non-interactive \
		"CALL apoc.export.json.query(\"$$CYPHER_QUERY\", '/var/lib/neo4j/export/$${export_filename}.pgjsonl', {jsonFormat: 'JSON_LINES', useTypes: true, stream: true})" && \
		echo "Query results exported successfully to data/export/$${export_filename}.pgjsonl" || \
		echo "Export failed. Check your query syntax and Neo4j connection."

.PHONY: jupyterlab
jupyterlab: ##@ Run jupyterlab
	@uv run kedro jupyter lab

.PHONY: kedro-viz
kedro-viz: ##@ Run kedro viz
	@uv run kedro viz --include-hooks