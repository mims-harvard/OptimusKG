# OptimusKG

--------------------------------------------------------------------------------

**Documentation**: [https://grence.ai/docs/optimuskg](https://grence.ai/docs/optimuskg)

**Source Code**: [https://github.com/mims-harvard/optimuskg](https://github.com/mims-harvard/optimuskg)

--------------------------------------------------------------------------------

Optimus is an opinionated, production-ready data pipeline designed to construct, validate, and maintain biomedical knowledge graphs following software engineering best practices.

Optimus is grounded in three foundational principles:

- **Ready-to-use**: Optimus comes with a set of pre-built processing nodes that unify many biomedical data sources into a single knowledge graph named OptimusKG.
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
> We leverage additional features of the Kedro framework, such as [namespaces](https://docs.kedro.org/en/latest/build/namespaces/), [kedro-viz](https://docs.kedro.org/projects/kedro-viz/en/latest/), [kedro-datasets](https://docs.kedro.org/projects/kedro-datasets/en/latest/) and catalog injection in [Jupyter notebooks](https://docs.kedro.org/en/latest/integrations-and-plugins/notebooks_and_ipython/kedro_and_notebooks/#exploring-the-kedro-project-in-a-notebook).
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
> If you have access to private datasets, place them in the appropriate subdirectories under `data/landing/`. The pipeline will automatically use them if present.
> 
> If you do not have access, the [`Origin Hook`](https://github.com/mims-harvard/optimuskg/blob/main/optimuskg/hooks/origin/origin_hooks.pya) will generate empty placeholder datasets in their place. This allows pipeline nodes that depend on both public and private data to run, even if the private data is missing. As a result, you can still execute the pipeline and work with the public portions of the data without interruption.

<!-- ## Using OptimusKG TODO: maybe we'll need to create a basic library to download the parquet data and load it in-memory as polars dataframes.-->

## Running Optimus

### Install dependencies

Optimus uses [`uv`](https://docs.astral.sh/uv/getting-started/installation/) as the project manager and [`docker`](https://docs.docker.com/engine/install/) to spin up the Neo4j database. 

> [!NOTE]
> Docker is not required if you don't need to export the graph in Neo4j-JSONL format.

Before running Optimus, you should sync its dependencies:

```console
$ uv sync

Resolved 234 packages in 1ms
Audited 225 packages in 0.42ms
```

> [!NOTE]
> The are some commands that leverage [GNU Make](https://www.gnu.org/software/make/). 
> The command line reference documentation can be viewed with `make help`.

### Generating the graph

Optimus is designed to generate a full knowledge graph in one command:

```console
$ uv run kedro run --to-nodes gold.pg_export --runner=ParallelRunner --async

[01/28/25 19:29:07] INFO     Using 'conf/logging.yml' as logging configuration. You can change this by setting the KEDRO_LOGGING_CONFIG environment variable accordingly.
[01/28/25 19:29:08] INFO     Kedro project optimuskg
[01/28/25 19:29:09] INFO     Using synchronous mode for loading and saving data. Use the --async flag for potential performance gains.
```

This will automatically download all the necessary data, store it in the `landing` layer, and execute the `bronze`, `silver`, and `gold` layers
to finally export the graph inside the `data/export/` folder.

> [!NOTE]
> It is recommended to use the [ParallelRunner](https://docs.kedro.org/en/latest/build/run_a_pipeline/#parallelrunner) 
> to run the nodes within a pipeline concurrently, and the [async](https://docs.kedro.org/en/latest/build/run_a_pipeline/#load-and-save-asynchronously) flag to reduce load and save time by using asynchronous mode.

> [!NOTE] 
> This will not only export the knowledge graph, but also all the intermediate datasets used to generate it. 
> The location of each dataset and their format is specified in the catalog.

Similarly, you can export the Neo4j-JSONL format with:

```console
$ uv run kedro run --to-nodes gold.neo4j_export --runner=ParallelRunner --async

[01/28/25 19:29:07] INFO     Using 'conf/logging.yml' as logging configuration. You can change this by setting the KEDRO_LOGGING_CONFIG environment variable accordingly.
[01/28/25 19:29:08] INFO     Kedro project optimuskg
[01/28/25 19:29:09] INFO     Using synchronous mode for loading and saving data. Use the --async flag for potential performance gains.
```

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
$ make neo4j-export
```

The data will be saved to `data/export/optimuskg.jsonl`.

To export the results of a specific Cypher query, run:

```console
$ CYPHER_QUERY="MATCH (d:Disease) RETURN d" make neo4j-export
```

The results will be saved to a file in `data/export/` with a filename derived from the query.

## Citation

The Optimus paper has been peer-reviewed in [Nature Biotechnology](). Before it was available as a pre-print at [arXiv]().

```
@article{vittor2025optimus,
  title={Building OptimusKG using the Optimus framework},
  author={Vittor, Lucas and Arango, Inaki and and Poloneur, Joaquin, and Noori, Ayush and Zitnik, Marinka},
  journal={Nature Scientific Data},
  doi={https://doi.org/<XXX>/<XXX>},
  URL={https://www.nature.com/articles/<XXX>},
  year={2025}
}
```

## License

Optimus is released under the [MIT LICENSE](LICENSE).
