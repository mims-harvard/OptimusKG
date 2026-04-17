# Contributing

## Setting up the development environment

We use [`uv`](https://github.com/astral-sh/uv) and [`docker`](https://www.docker.com/) for local development.

1. Install `uv` by following the [uv installation instructions](https://docs.astral.sh/uv/getting-started/installation/).
2. Install `docker` by following the [Docker installation instructions](https://docs.docker.com/engine/install/).
3. Install all dependencies:

    ```console
    $ uv sync --all-groups

    Resolved 234 packages in 1ms
    Audited 225 packages in 0.42ms
    ```

4. Install the pre-commit hooks:

    ```console
    $ uv run pre-commit install
    pre-commit installed at .git/hooks/pre-commit
    ```

> [!TIP]
> Run `make help` for a list of all available Make commands.

## Code quality

All code quality checks are run through `make`. These same checks run in CI on every pull request.

| Command | Description |
| --- | --- |
| `make format` | Auto-format code with `ruff format` and fix lint violations with `ruff check --fix`. |
| `make lint` | Check formatting and lint rules without making changes (used in CI). |
| `make ty` | Run `ty` for static type checking across `optimuskg/` and `cli/`. |
| `make interrogate` | Check docstring coverage against the threshold defined in `pyproject.toml`. |

## CI requirements

Every pull request must pass three checks before merging:

1. **Type checking and docstring coverage** — `make ty` and `make interrogate` must pass.
2. **Formatting** — `make lint` and spell checking with `typos` must pass. Run `make format` locally to fix issues before pushing.
3. **Semver bump** — the `version` field in `pyproject.toml` must be incremented on every PR. The CI job compares the version between the PR branch and `main` and fails if it has not changed.

## Running tests

Tests use [`pytest`](https://docs.pytest.org/en/stable/) and are run through [`hatch`](https://hatch.pypa.io/latest/) to pick up the pre-configured flags in `pyproject.toml`:

```console
$ uv tool run hatch run pytest
```

## Editing pipeline nodes

The OptimusKG data pipeline enforces strict synchronization between node files and catalog YAML files. When you edit any Python node file under `optimuskg/pipelines/*/nodes/`, you must also update the corresponding catalog entries and rerun all downstream nodes.

Follow the four-step sync workflow:

1. Identify the catalog entries affected by your change in `conf/base/catalog/`.
2. Update the dataset ID, filepath, or schema as needed.
3. Rerun the node and sync the catalog:
    ```console
    $ uv run kedro run --from-nodes=<node_name>
    $ uv run cli sync-catalog --dataset <catalog_id>
    ```
4. Cascade downstream — rerun all nodes that depend on the changed output.

> [!IMPORTANT]
> Never delete the `checksum` field from a catalog entry. If the output of a node changes, recompute it with `uv run cli sync-catalog --dataset <catalog_id>`.

## CLI utilities

The pipeline ships a [Typer](https://typer.tiangolo.com/)-based CLI for common maintenance tasks:

```console
$ uv run cli --help
```

### `sync-catalog` — Synchronize catalog schemas and checksums

For **ParquetDataset** entries the command reads the Parquet file on disk and updates the YAML schema specification. For any dataset with a `metadata.checksum` field it recomputes the BLAKE2b checksum and updates the catalog YAML (using regex replacement to preserve formatting, comments, and OmegaConf syntax).

```console
# Sync all schemas and checksums
$ uv run cli sync-catalog

# Preview changes without writing files
$ uv run cli sync-catalog --dry-run

# Validate without updating (useful in CI)
$ uv run cli sync-catalog --validate

# Target a specific layer
$ uv run cli sync-catalog --layer bronze

# Target a specific dataset
$ uv run cli sync-catalog --dataset bronze.opentargets.disease
```

| Option | Short | Description |
| --- | --- | --- |
| `--layer` | `-l` | Target layer: `landing`, `bronze`, `silver`, or `all` (default: `all`). |
| `--dataset` | `-d` | Specific dataset name (e.g., `bronze.opentargets.disease`). |
| `--validate` | `-v` | Validate schemas and checksums without updating files. |
| `--dry-run` | `-n` | Preview changes without writing files. |
| `--catalog-dir` | | Path to the catalog directory (default: `conf/base/catalog`). |
| `--data-dir` | | Path to the data directory (default: `data`). |
