# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OptimusKG is a Kedro-based data pipeline for building biomedical knowledge graphs. It follows a medallion architecture (landing -> bronze -> silver -> gold) to transform raw biomedical data from sources like OpenTargets, DrugBank, BGEE, DisGeNET, CTD, and Reactome into a unified knowledge graph exported as CSV, Parquet, or Neo4j-JSONL.

## Common Commands

```bash
# Install dependencies
uv sync --all-groups

# Install pre-commit hooks
uv tool run pre-commit install

# Format code
make format

# Lint (CI check)
make lint

# Type checking (CI check)
make ty

# Docstring coverage (CI check)
make interrogate

# Run the full pipeline
uv run kedro run --to-nodes gold.export_kg --runner=optimuskg.runners.FixedParallelRunner --async

# Run a specific pipeline or node
uv run kedro run --pipeline bronze
uv run kedro run --nodes <node_name>

# Run downstream from a node (cascade)
uv run kedro run --from-nodes=<node_name>

# Dry run (list nodes without executing)
uv run kedro run --from-nodes=<node_name> --runner=optimuskg.runners.DryRunner

# CLI utilities
uv run cli --help
uv run cli checksum           # Validate file checksums
uv run cli sync-catalog       # Sync schema + checksum from parquet to catalog YAML
uv run cli metrics            # Generate graph metrics

# Run tests
uv tool run hatch run pytest

# Start Neo4j
make neo4j

# Kedro visualization
make kedro-viz
```

## CI Requirements

PRs must pass three checks:
1. **Type checking + docstring coverage**: `make ty` and `make interrogate`
2. **Formatting**: typos spell check + ruff format/lint
3. **Semver bump**: version in `pyproject.toml` must be incremented every PR

## Architecture

### Pipeline Layers (Medallion Pattern)

- **Bronze** (`optimuskg/pipelines/bronze/`): Extracts and standardizes raw data from external sources. Each source has its own node file under `nodes/`. Outputs standardized Polars DataFrames as Parquet.
- **Silver** (`optimuskg/pipelines/silver/`): Consolidates entities across sources into unified node tables (Gene, Drug, Disease, Protein, Anatomy, Pathway) and builds ~31 relationship edge types.
- **Gold** (`optimuskg/pipelines/gold/`): Exports the final knowledge graph via BioCypher in configured formats.

### Key Framework Patterns

**Kedro catalog** (`conf/base/catalog/`): Every dataset is defined in YAML organized by layer and source. Each entry specifies filepath, format, schema, and metadata (including `checksum` and `origin`). Nodes receive/return data through catalog-defined datasets.

**Pipeline nodes** (`optimuskg/pipelines/*/nodes/`): Pure functions that take Polars DataFrames as input and return DataFrames. Registered in each layer's `pipeline.py` with namespace prefixes.

**Hooks** (`optimuskg/hooks/`): Execute during pipeline lifecycle:
- `ChecksumHooks` — validates file integrity against catalog checksums
- `QualityChecksHooks` — validates snake_case columns, non-null IDs, unique IDs
- `OriginHooks` — auto-downloads data from external sources before pipeline runs

**Custom runners** (`optimuskg/runners/`): `FixedParallelRunner` for parallel execution, `DryRunner` for listing nodes without running.

**Custom datasets** (`optimuskg/datasets/`): `OWLDataset`, `LXMLDataset`, `ZipDataset`, `SQLDumpQueryDataset`, `JsonDataset`, `ParquetDataset` — all extend Kedro's `AbstractVersionedDataset`.

### Data Processing

All transformations use **Polars** (not Pandas). Custom Polars type parsing in `optimuskg/utils.py` for YAML schema definitions.

### Configuration

- `conf/base/catalog/` — dataset definitions by layer
- `conf/base/parameters.yml` — runtime parameters (export formats)
- `conf/base/biocypher/` — BioCypher schema and Neo4j export config
- `conf/local/` — local overrides (gitignored)

## Critical Rules

**When editing a node file, you MUST also update the corresponding catalog YAML files.** Use the `/node-catalog-sync` skill (see below) which provides the full sync workflow including path mappings, rerunning nodes, and cascading downstream.

**Never delete the `checksum` property in the catalog.** If code that generates a file changes, rerun the node and sync: `uv run cli sync-catalog --dataset {catalog_id}`.

**When editing a node, rerun all downstream nodes.** Use `uv run kedro run --from-nodes={node_name}` then `uv run cli sync-catalog` to update all affected checksums.

## Skills (`.agents/skills/`)

Three skills are available for specialized workflows:

### `/node-catalog-sync`
Enforces synchronization between Kedro node files and catalog YAML files. **Use when editing any Python node file** under `optimuskg/pipelines/*/nodes/`, including modifying `run()` functions, changing `node()` inputs/outputs, adding/removing/renaming DataFrame columns, or changing column types. Provides the 4-step sync workflow: identify affected catalog entries, update dataset ID/filepath, rerun node and sync catalog, cascade downstream.

### `/scientific-visualization`
Creates publication-quality figures with matplotlib/seaborn/plotly. Includes journal-specific style presets (Nature, Science, Cell), colorblind-safe palettes, multi-panel layouts, and export utilities. Use when creating or improving figures for manuscripts. Assets include `.mplstyle` files and helper scripts (`figure_export.py`, `style_presets.py`).

### `/skill-creator`
Guide for creating new skills that extend Claude's capabilities. Use when building or updating a skill. Provides the full skill creation lifecycle: understanding requirements, planning resources, initializing via `scripts/init_skill.py`, editing, packaging via `scripts/package_skill.py`, and iterating.
