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

[Learn the basics of Optimus](https://grence.ai/docs/optimuskg)

At an architectural level, Optimus consists of the following components:

| Component | Description |
| ---- | --- |
| [**catalog**](https://grence.ai/docs/optimuskg/the-catalog) | The single source of truth of all datasets, their schemas, their format, and their metadata. |
| [**dataset**](https://docs.kedro.org/en/unreleased/extend/how_to_create_a_custom_dataset/) | An abstraction that handles file formats, storage locations, and persistence logic. |
| [**node**](https://docs.kedro.org/en/unreleased/getting-started/kedro_concepts/#node) | A pure Python function whose output value follows solely from its input values. |
| [**pipeline**](https://docs.kedro.org/en/unreleased/getting-started/kedro_concepts/#pipeline) | A sequence of nodes wired into a DAG-based workflow, organized by the datasets they consume and produce. |
| [**layer**]() | Follows the medallion architecture data design pattern to logically organize the data. There are 4 layers: landing, bronze, silver, and gold.|
| [**parameters**](https://docs.kedro.org/en/unreleased/configure/parameters/) | Used to define constants for filtering the data across the construction process. |
| [**provider**]() | An abstraction that provides versioned, automatic data downloads from different data sources. |
| [**hook**](https://docs.kedro.org/en/unreleased/extend/hooks/introduction/) | Mechanism that allows injection of custom behavior into the core execution flow, such as before a node runs (for example, for checksum checks). |
| [**conf**]() | A mechanism that separates _code_ from _settings_, defining the catalog, parameters, logging configuration, and ontology harmonization across different environments (base, local, prod, etc.). |

> [!NOTE]
> We leverage additional features of the Kedro framework, such as namespaces, kedro-viz, and catalog injection in Jupyter notebooks.
> Optimus also comes with a command-line interface and quality-of-life tooling for spinning up Neo4j, exporting slices of the graph, etc.

## Releases and Contributing

OptimusKG is a biomedical Labeled Property Graph built using the Optimus pipeline. Each release provides the graph data in several formats, each tailored for different use cases:

| Format | File | Description |
| ---- | --- | --- |
| **CSV** | `optimuskg.csv.zip` | Contains the headers and partitioned data files for each node and edge type. This is useful for bulk-importing the graph into a [Neo4j database](https://neo4j.com/docs/getting-started/data-import/). |
| **PG-JSONL** | `optimuskg.pg.jsonl` | A single file representing the graph in the standardized [Property Graph Exchange Format (PG)](https://pg-format.github.io/specification/). This human-readable format is well-suited for data exchange, and situations where a self-contained graph representation is needed. |
| **Neo4j-JSONL** | `optimuskg.jsonl` | A direct JSON lines export from a Neo4j instance of OptimusKG. This format is useful for interoperability with other tools in the Neo4j ecosystem, but is much large in size than the PG-JSONL format. |
| **Parquet** | `nodes-parquet.zip` and `edges-parquet.zip` | Provide all the nodes and edges as separate [Apache Parquet](https://parquet.apache.org/) files. As a columnar storage format, Parquet is highly efficient for large-scale data analysis and is recommended for data science and machine learning workflows with tools like [Apache Spark](https://spark.apache.org/) and [Polars](https://pola.rs/). |
| **Diamond** | `optimuskg.diamond` | A custom, compressed binary format of the full graph. It offers the smallest file size, making it ideal for efficient storage and network transfer. Diamond is developed by the same authors of Optimus. |

Each release includes a comprehensive graph report that contains:

- **Overview**: Total elements, number of non-null properties, number of nodes, and number of edges.
- **Node metrics by type**: Number of nodes, their percentage, average number of properties and standard deviation, average degree and standard deviation.
- **Edge metrics by type**: Number of edges, their percentage, average number of properties and standard deviation.
- **Graph topology**: Number of directed, undirected, bi-directional, duplicated, and loop edges.

> [!NOTE]
> Distributed OptimusKG data files contain only publicly available data.
> If you want to have the full OptimusKG data, you'll need to run the Optimus 
> pipeline manually with the required private data at hand. See [using private data]().

## Using OptimusKG

## Running Optimus



## Citation


## License

Optimus is released under the [MIT LICENSE](LICENSE).
