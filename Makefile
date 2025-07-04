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
	@uv run ruff check --fix --exit-non-zero-on-fix optimuskg cli
	@uv run ruff format optimuskg cli

.PHONY: lint
lint: ##@ Lint code
	@uv run ruff check optimuskg cli
	@uv run ruff format --check optimuskg cli

.PHONY: mypy
mypy: ##@ Run mypy
	@uv run mypy --install-types --config-file pyproject.toml --non-interactive --package optimuskg
	@uv run mypy --config-file pyproject.toml tests
	@uv run mypy --config-file pyproject.toml cli

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
		--verbose \
		--delimiter=";" \
		--array-delimiter="|" \
		--quote='"' \
		--overwrite-destination=true \
		--nodes="/import/Disease-header.csv,/import/Disease-part.*" \
		--nodes="/import/Phenotype-header.csv,/import/Phenotype-part.*" \
		--nodes="/import/Drug-header.csv,/import/Drug-part.*" \
        --nodes="/import/Gene-header.csv,/import/Gene-part.*" \
        --nodes="/import/Anatomy-header.csv,/import/Anatomy-part.*" \
		--nodes="/import/Exposure-header.csv,/import/Exposure-part.*" \
		--nodes="/import/BiologicalProcess-header.csv,/import/BiologicalProcess-part.*" \
		--nodes="/import/CellularComponent-header.csv,/import/CellularComponent-part.*" \
		--nodes="/import/MolecularFunction-header.csv,/import/MolecularFunction-part.*" \
		--nodes="/import/Pathway-header.csv,/import/Pathway-part.*" \
        --relationships="/import/AnatomyProtein-header.csv,/import/AnatomyProtein-part.*" \
        --relationships="/import/ExposureExposure-header.csv,/import/ExposureExposure-part.*" \
        --relationships="/import/ExposureProtein-header.csv,/import/ExposureProtein-part.*" \
		--relationships="/import/DiseaseProtein-header.csv,/import/DiseaseProtein-part.*" \
		--relationships="/import/DiseaseDisease-header.csv,/import/DiseaseDisease-part.*" \
		--relationships="/import/ExposureDisease-header.csv,/import/ExposureDisease-part.*" \
		--relationships="/import/DrugProtein-header.csv,/import/DrugProtein-part.*" \
		--relationships="/import/DrugDrug-header.csv,/import/DrugDrug-part.*" \
		--relationships="/import/DrugDisease-header.csv,/import/DrugDisease-part.*" \
		--relationships="/import/DrugPhenotype-header.csv,/import/DrugPhenotype-part.*" \
		--relationships="/import/PhenotypePhenotype-header.csv,/import/PhenotypePhenotype-part.*" \
		--relationships="/import/PhenotypeProtein-header.csv,/import/PhenotypeProtein-part.*" \
		--relationships="/import/BiologicalProcessProtein-header.csv,/import/BiologicalProcessProtein-part.*" \
		--relationships="/import/CellularComponentProtein-header.csv,/import/CellularComponentProtein-part.*" \
		--relationships="/import/MolecularFunctionProtein-header.csv,/import/MolecularFunctionProtein-part.*" \
		--relationships="/import/PathwayPathway-header.csv,/import/PathwayPathway-part.*" \
		--relationships="/import/PathwayProtein-header.csv,/import/PathwayProtein-part.*" \
		--relationships="/import/ExposureBiologicalProcess-header.csv,/import/ExposureBiologicalProcess-part.*" \
		--relationships="/import/ExposureMolecularFunction-header.csv,/import/ExposureMolecularFunction-part.*" \
		--relationships="/import/ExposureCellularComponent-header.csv,/import/ExposureCellularComponent-part.*" \
		--relationships="/import/MolecularFunctionMolecularFunction-header.csv,/import/MolecularFunctionMolecularFunction-part.*" \
		--relationships="/import/BiologicalProcessBiologicalProcess-header.csv,/import/BiologicalProcessBiologicalProcess-part.*" \
		--relationships="/import/CellularComponentCellularComponent-header.csv,/import/CellularComponentCellularComponent-part.*" \
		--relationships="/import/DiseasePhenotype-header.csv,/import/DiseasePhenotype-part.*" \
		--relationships="/import/AnatomyAnatomy-header.csv,/import/AnatomyAnatomy-part.*" \

.PHONY: neo4j-export
neo4j-export: ##@ Export Neo4j database to JSONL format. Set CYPHER_QUERY env var for specific query, otherwise exports all. Example: CYPHER_QUERY="MATCH (d:Disease) RETURN d" make neo4j-export
	@mkdir -p data/export
	@if [ -z "$$CYPHER_QUERY" ]; then \
		echo "Exporting entire database..."; \
		file="optimuskg.jsonl"; \
		call="apoc.export.json.all('/var/lib/neo4j/export/$$file', {jsonFormat: 'JSON_LINES', useTypes: true})"; \
	else \
		echo "Exporting query results..."; \
		file="$$(echo "$$CYPHER_QUERY" | tr ' ' '_' | tr -cd '[:alnum:]_' | cut -c1-30).jsonl"; \
		call="apoc.export.json.query(\"$$CYPHER_QUERY\", '/var/lib/neo4j/export/$$file', {jsonFormat: 'JSON_LINES', useTypes: true})"; \
	fi; \
	docker compose exec neo4j cypher-shell --non-interactive "CALL $$call" && \
		echo "Exported to data/export/$$file" || echo "Export failed"

.PHONY: jupyterlab
jupyterlab: ##@ Run jupyterlab
	@uv run kedro jupyter lab

.PHONY: kedro-viz
kedro-viz: ##@ Run kedro viz
	@uv run kedro viz --include-hooks