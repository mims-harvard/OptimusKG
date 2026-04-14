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

.PHONY: ty
ty: ##@ Run ty type checker
	@uv run ty check optimuskg cli

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
	@rm -rf .ty
	@rm -rf .venv
	@rm -rf .pytest_cache
	@rm -rf .ruff_cache
	@rm -rf htmlcov
	@rm -f .coverage*
	@rm -rf target
	@rm -rf .viz
	@rm -rf build

.PHONY: rm-data
rm-data: ##@ Remove the data directory
	@find data/ -type f -not -name ".gitkeep" -delete
	@find data/ -type d -empty -not -name ".gitkeep" -delete

.PHONY: download-landing
download-landing: ##@ Clean data directory, and download the landing layer into disk. Example: URL="https://example-blob.com/landing.zip" [DEST=<optional-dir>] make landing
	@$(MAKE) rm-data
	@./scripts/download_landing.sh

.PHONY: neo4j
neo4j: ##@ Run the Neo4j container
	@docker compose up -d 

.PHONY: docker-datasets-up
docker-datasets-up: ##@ Spin up docker for the datasets
	@docker compose -f optimuskg/datasets/sqldump_query_dataset/docker-compose.yaml up -d

.PHONY: docker-datasets-down
docker-datasets-down: ##@ Stop docker for the datasets
	@docker compose -f optimuskg/datasets/sqldump_query_dataset/docker-compose.yaml down

.PHONY: jupyterlab
jupyterlab: ##@ Run jupyterlab
	@uv run kedro jupyter lab

.PHONY: kedro-viz
kedro-viz: ##@ Run kedro viz
	@uv run kedro viz --include-hooks

# -----------------------------------------------
##@ Documentation
# -----------------------------------------------

.PHONY: docs-dev
docs-dev: ##@ Start the documentation dev server
	@cd docs && pnpm dev

.PHONY: docs-build
docs-build: ##@ Build the documentation site
	@cd docs && pnpm build

.PHONY: docs-preview
docs-preview: ##@ Preview the documentation production build
	@cd docs && pnpm start

.PHONY: docs-install
docs-install: ##@ Install documentation dependencies
	@cd docs && pnpm install
