<p align="center">
  <img src="docs/public/full-logo.jpg" alt="OptimusKG" width="500">
</p>

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![DOI](https://img.shields.io/badge/DOI-10.7910%2FDVN%2FIYNGEV-blue)](https://doi.org/10.7910/DVN/IYNGEV)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Docs](https://img.shields.io/badge/docs-optimuskg.ai-blue)](https://optimuskg.ai)

## OptimusKG

OptimusKG is a multimodal biomedical labeled property graph built from structured and semi-structured resources to preserve factual, type-specific metadata across molecular, anatomical, clinical, and environmental domains. OptimusKG contains 190,531 nodes across 10 entity types, 21,813,816 edges across 26 relation types, and 67,249,863 property instances encoding 110,276,843 values across 150 distinct property keys, derived from 18 ontologies and controlled vocabularies.


## Highlights

- A [multimodal biomedical labeled property graph](https://optimuskg.ai) spanning molecular, anatomical,
  clinical, and environmental domains — 190,531 nodes and 21,813,816 edges across 10 entity types and
  26 relation types.
- Integrates [65 heterogeneous primary datasets](#graph-at-a-glance) and
  [18 ontologies and controlled vocabularies](#graph-at-a-glance), all harmonized against the
  [Biolink Model](https://biolink.github.io/biolink-model/).
- [70.0% of edges](#releases-and-contributing) independently supported by scientific literature,
  validated using [PaperQA3](https://github.com/Future-House/paper-qa) — a multimodal AI agent for
  deep literature research.
- Generates the full knowledge graph in a [single command](#generating-the-knowledge-graph), with
  automatic data downloads, checksum validation, and quality checks at every layer.
- Reproducible and infra-agnostic — all transformations are deterministic and verified through
  BLAKE2b checksums.
- Extensible via the [Kedro framework](https://kedro.org/) — uniform project template, dataset
  abstractions, catalog management, and pipeline assembly.
- Distributed as [Apache Parquet](https://parquet.apache.org/) files, ready for data science, ML,
  and graph AI workflows.
- Follows the [FAIR principles](https://www.go-fair.org/fair-principles/) with full provenance
  tracking and standardized identifiers across all nodes and edges.
- Supports parallel execution with async data loading for high-performance graph construction at
  scale.

## Data pipeline

At an architectural level, the OptimusKG data pipeline consists of the following components:

| Component | Description |
| ---- | --- |
| [**catalog**](https://optimuskg.ai/the-catalog) | The single source of truth of all datasets, their schemas, their format, and their metadata. |
| [**dataset**](https://docs.kedro.org/en/unreleased/extend/how_to_create_a_custom_dataset/) | An abstraction that handles file formats, storage locations, and persistence logic. |
| [**node**](https://docs.kedro.org/en/unreleased/getting-started/kedro_concepts/#node) | A pure Python function whose output value follows solely from its input values. |
| [**pipeline**](https://docs.kedro.org/en/unreleased/getting-started/kedro_concepts/#pipeline) | A sequence of nodes wired into a DAG-based workflow, organized by the datasets they consume and produce. |
| [**layer**]() | Follows the medallion architecture data design pattern to logically organize the data. There are 4 layers: landing, bronze, silver, and gold.|
| [**parameters**](https://docs.kedro.org/en/unreleased/configure/parameters/) | Used to define constants for filtering the data across the construction process. |
| [**provider**]() | An abstraction that provides versioned, automatic data downloads from different data sources. |
| [**hook**](https://docs.kedro.org/en/unreleased/extend/hooks/introduction/) | Mechanism that allows injection of custom behavior into the core execution flow, such as before a node runs (for example, for checksum checks). |
| [**conf**]() | A mechanism that separates _code_ from _settings_, defining the catalog, parameters, logging configuration, and ontology harmonization across different environments (base, local, prod, etc.). |

> [!NOTE]
> We leverage additional features of the Kedro framework, such as [namespaces](https://docs.kedro.org/en/latest/build/namespaces/), [kedro-viz](https://docs.kedro.org/projects/kedro-viz/en/latest/), [kedro-datasets](https://docs.kedro.org/projects/kedro-datasets/en/latest/) and catalog injection in [Jupyter notebooks](https://docs.kedro.org/en/latest/integrations-and-plugins/notebooks_and_ipython/kedro_and_notebooks/#exploring-the-kedro-project-in-a-notebook).
> The OptimusKG data pipeline also comes with a command-line interface and quality-of-life tooling for spinning up Neo4j, exporting slices of the graph, etc.

## Releases and Contributing

OptimusKG is a biomedical Labeled Property Graph built using the OptimusKG data pipeline. Each release provides the graph data in several formats, each tailored for different use cases:

| Format | Description |
| ---- | --- |
| **Neo4j-JSONL** | A direct JSON lines export from a Neo4j instance of OptimusKG. Useful for interoperability with other tools in the Neo4j ecosystem. |
| **Parquet** | Partitioned [Apache Parquet](https://parquet.apache.org/) files for each node and edge type, plus unified `nodes.parquet` and `edges.parquet` files. Recommended for data science and machine learning workflows with tools like [Apache Spark](https://spark.apache.org/) and [Polars](https://pola.rs/). |

Each release includes a comprehensive graph report that contains:

- **Overview**: Total elements, number of non-null properties, number of nodes, and number of edges.
- **Node metrics by type**: Number of nodes, their percentage, average number of properties and standard deviation, average degree and standard deviation.
- **Edge metrics by type**: Number of edges, their percentage, average number of properties and standard deviation.
- **Graph topology**: Number of directed, undirected, bi-directional, duplicated, and loop edges.

Edge quality in OptimusKG is independently validated using [PaperQA3](https://github.com/Future-House/paper-qa), a multimodal AI agent for deep literature search. PaperQA3 identified supporting evidence for **70.0%** of sampled edges, while **83.4%** of sampled false edges received no supporting evidence — indicating a high completion rate relative to the published scientific literature.

> [!NOTE]
> Distributed OptimusKG data files contain only publicly available data.
> If you have access to private datasets, place them in the appropriate subdirectories under `data/landing/`. The pipeline will automatically use them if present.
>
> If you do not have access, the [`Origin Hook`](https://github.com/mims-harvard/optimuskg/blob/main/optimuskg/hooks/origin/origin_hooks.py) will generate empty placeholder datasets in their place. This allows pipeline nodes that depend on both public and private data to run, even if the private data is missing. As a result, you can still execute the pipeline and work with the public portions of the data without interruption.

## Running the OptimusKG data pipeline

### Install dependencies

The OptimusKG data pipeline requires **Python 3.12 or higher**, and uses [`uv`](https://docs.astral.sh/uv/getting-started/installation/) as the project manager and [`docker`](https://docs.docker.com/engine/install/) to spin up the Neo4j database.

> [!NOTE]
> Docker is not required if you don't need to export the graph in Neo4j-JSONL format.

Before running the OptimusKG data pipeline, sync its dependencies:

```console
$ uv sync

Resolved 234 packages in 1ms
Audited 225 packages in 0.42ms
```

> [!NOTE]
> There are some commands that leverage [GNU Make](https://www.gnu.org/software/make/).
> The command line reference documentation can be viewed with `make help`.

> [!TIP]
> Run `make help` for a list of available Make commands, and `uv run cli --help` for additional CLI utilities (e.g., checksum validation, metrics generation).

### Generating the knowledge graph

The OptimusKG data pipeline is designed to generate the full knowledge graph in one command:

```console
$ uv run kedro run --to-nodes gold.export_kg --runner=optimuskg.runners.FixedParallelRunner --async

[01/28/25 19:29:07] INFO     Using 'conf/logging.yml' as logging configuration. You can change this by setting the KEDRO_LOGGING_CONFIG environment variable accordingly.
[01/28/25 19:29:08] INFO     Kedro project optimuskg
[01/28/25 19:29:09] INFO     Using synchronous mode for loading and saving data. Use the --async flag for potential performance gains.
```

This will automatically download all the necessary data, store it in the `landing` layer, and execute the `bronze`, `silver`, and `gold` layers
to finally export the graph inside the `data/gold/kg/` folder.

> [!NOTE]
> It is recommended to use the `optimuskg.runners.FixedParallelRunner`
> to run the nodes within a pipeline concurrently, and the [async](https://docs.kedro.org/en/latest/build/run_a_pipeline/#load-and-save-asynchronously) flag to reduce load and save time by using asynchronous mode. The original [ParallelRunner](https://docs.kedro.org/en/latest/build/run_a_pipeline/#parallelrunner) contains a bug that prevents it from running any validation checks.

> [!NOTE]
> This will not only export the knowledge graph, but also all the intermediate datasets used to generate it.
> The location of each dataset and their format is specified in the catalog.

> [!TIP]
> Export formats (Parquet, Neo4j) can be configured in `conf/base/parameters.yml` under `gold.export_formats`.

Then, you can spin up a Neo4j database with the graph data simply by running:

```console
$ make neo4j

[+] Running 2/2
 ✔ Network optimuskg_default Created 
 ✔ Container neo4j           Started 
```

> [!NOTE]
> This will start a Neo4j container in the background. You can access the Neo4j Browser at [http://localhost:7474/browser/preview/](http://localhost:7474/browser/preview/).

You can export the entire database or the results of a specific query once the container is running.

To export the entire database to a Neo4j-JSONL file, run:

```console
make neo4j-export
```

The data will be saved to `data/export/optimuskg.jsonl`.

To export the results of a specific Cypher query, run:

```console
CYPHER_QUERY="MATCH (d:Disease) RETURN d" make neo4j-export
```

The results will be saved to a file in `data/export/` with a filename derived from the query.

## CLI Utilities

The OptimusKG data pipeline ships a Typer-based CLI for common maintenance tasks. After installing dependencies you can run it with:

```console
uv run cli --help
```

### `sync-catalog` — Synchronize catalog schemas and checksums

For **ParquetDataset** entries the command reads the parquet file on disk and updates the YAML schema specification.
For any dataset with a `metadata.checksum` field it recomputes the BLAKE2b checksum and updates the catalog YAML (using regex replacement to preserve formatting, comments, and OmegaConf syntax).

```console
# Sync all schemas and checksums (landing, bronze, silver)
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

## Citation

If you use OptimusKG in your research, please cite:

```bibtex
@article{vittor2025optimus,
  title={OptimusKG: Unifying biomedical knowledge in a modern multimodal graph},
  author={Vittor, Lucas and Noori, Ayush and Arango, I{\~n}aki and Polonuer, Joaqu{\'\i}n and Rodriques, Sam and White, Andrew and Clifton, David A. and Zitnik, Marinka},
  journal={Nature Scientific Data},
  year={2026}
}
```

## License

The OptimusKG codebase is released under the [MIT License](LICENSE). OptimusKG integrates multiple primary data resources, each of which is subject to its own license and terms of use. These terms may impose restrictions on redistribution, commercial use, or downstream applications of the resulting knowledge graph or its subsets. Some resources provide data under academic or noncommercial licenses, while others may impose attribution or usage requirements. As a result, use of OptimusKG may be partially restricted depending on the specific data components included in a given instantiation. Users are responsible for reviewing and complying with the license and terms of use of each primary dataset, as specified by the original data providers. OptimusKG does not alter or override these source-specific licensing conditions.
