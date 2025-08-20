--------------------------------------------------------------------------------

**Documentation**: [https://grence.ai/docs/optimuskg](https://grence.ai/docs/optimuskg)

**Source Code**: [https://github.com/mims-harvard/optimuskg](https://github.com/mims-harvard/optimuskg)

--------------------------------------------------------------------------------

Optimus is an opinionated, production-ready data pipeline designed to construct, validate, and maintain biomedical knowledge graphs following software engineering best practices.

Optimus is grounded in three foundational principles:

- **Ready-to-use**: Optimus comes with a set of pre-built processing nodes that unify many biomedical data sources into a single knowledge graph (OptimusKG).
- **Reproducible**: All data transformations are deterministic, validated through checksum checks, and infra-agnostic.
- **Extensible**: Optimus is a superset of the [Kedro framework](https://kedro.org/) (hosted by the [Linux Foundation](https://lfaidata.foundation/)), providing a uniform project template, dataset abstraction, configuration management, and pipeline assembly.

## More about Optimus

At an architectural level, Optimus consists of the following components:

| Component | Description |
| ---- | --- |
| [**catalog**](https://grence.ai/docs/optimuskg/the-catalog) | The single source of truth of all datasets, their schemas, their format, and their metadata. |
| [**dataset**](https://docs.kedro.org/en/unreleased/extend/how_to_create_a_custom_dataset/) | An abstraction that handles file formats, storage locations, and persistence logic. |
| [**nodes**](https://docs.kedro.org/en/unreleased/getting-started/kedro_concepts/#node) | A pure Python function whose output value follows solely from its input values. |
| [**pipelines**](https://docs.kedro.org/en/unreleased/getting-started/kedro_concepts/#pipeline) | A sequence of nodes wired into a DAG-based workflow, organized by the datasets they consume and produce. |
| [**layers**]() | Follows the medallion architecture data design pattern to logically organize the data. There are 4 layers: landing, bronze, silver, and gold.|
| [**parameters**](https://docs.kedro.org/en/unreleased/configure/parameters/) | Used to define constants for filtering the data across the construction process. |
| [**providers**]() | An abstraction that provides versioned, automatic data downloads from different data sources. |
| [**hooks**](https://docs.kedro.org/en/unreleased/extend/hooks/introduction/) | Mechanism that allows injection of custom behavior into the core execution flow, such as before a node runs (for example, for checksum checks). |
| [**conf**]() | Entrypoint for defining the catalog, the logging settings, and different environments (base, local, prod, etc). |


