# OptimusKG

[![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)](https://github.com/mims-harvard/optimuskg)
[![Release](https://github.com/mims-harvard/optimuskg/actions/workflows/cd.yml/badge.svg)](https://github.com/mims-harvard/optimuskg)

## Installation

Prerequisites for this project are:

- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- [docker](https://docs.docker.com/engine/install/)

Install dependencies with:

```console
$ uv sync --all-groups

Resolved 218 packages in 3ms
Audited 215 packages in 0.28ms
```

## Run it

### Download landing files

Most landing files are downloaded automatically. The catalog entries have an optional `origin` key in their metadata that specifies where to download the file from.

However, there are some files that are not downloaded automatically, so you need to download them manually. Ask the team for the files and put them in the `data/landing` folder as follows:

```console
optimuskg/
└── data/
    └── landing/
        └── drugbank/
        |   └── full database.xml
        |   └── drugbank_all_carrier_polypeptide_ids.csv
        |   └── drugbank_all_enzyme_polypeptide_ids.csv
        |   └── drugbank_all_target_polypeptide_ids.csv
        |   └── drugbank_all_transporter_polypeptide_ids.csv
        └── disgenet/
            └── curated_gene_disease_associations.tsv
```

### Set up Neo4j volume permissions

Before running the project, ensure proper permissions are set for the Neo4j import volume. This allows both the Neo4j container and Kedro to write the BioCypher GraphML file. Run:

```console
$ sudo chmod -R 777 data/gold/neo4j && sudo chown -R $(id -u):$(id -g) data/gold/neo4j
```

### Run the project

```console
$ uv run kedro run --async
```

### How to run your Kedro pipeline

You can run your the pipeline with:

```console
$ uv run kedro run

[01/28/25 19:29:07] INFO     Using 'conf/logging.yml' as logging configuration. You can change this by setting the KEDRO_LOGGING_CONFIG environment variable accordingly.
[01/28/25 19:29:08] INFO     Kedro project optimuskg
[01/28/25 19:29:09] INFO     Using synchronous mode for loading and saving data. Use the --async flag for potential performance gains.
```

You can also run specific nodes:

```console
$ uv run kedro run --nodes bronze.ctd

[01/28/25 19:31:35] INFO     Using 'conf/logging.yml' as logging configuration. You can change this by setting the KEDRO_LOGGING_CONFIG environment variable
                             accordingly.
                    INFO     Kedro project optimuskg                                                                                                          session.py:329
[01/28/25 19:31:36] INFO     Using synchronous mode for loading and saving data. Use the --async flag for potential performance gains.               sequential_runner.py:68
                             https://docs.kedro.org/en/stable/nodes_and_pipelines/run_a_pipeline.html#load-and-save-asynchronously
                    INFO     Loading data from landing.ctd.ctd_exposure_events (CSVDataset)...                                                           data_catalog.py:389
                    INFO     Running node: ctd: process_ctd([landing.ctd.ctd_exposure_events]) -> [bronze.ctd.ctd_exposure_events]                               node.py:367
                    INFO     Saving data to bronze.ctd.ctd_exposure_events (CSVDataset)...                                                               data_catalog.py:431
[01/28/25 19:31:37] INFO     Completed 1 out of 1 tasks                                                                                              sequential_runner.py:93
                    INFO     Pipeline execution completed successfully.
```

Or a specific pipeline:

```console
$ uv run kedro run --pipeline bronze
[05/28/25 11:28:38] INFO     Using 'conf/logging.yml' as logging configuration. You can change this by setting the KEDRO_LOGGING_CONFIG __init__.py:270
                             environment variable accordingly.                                                                                         
                    INFO     Kedro project optimuskg                                                                                     session.py:329
[05/28/25 11:28:39] INFO     Using synchronous mode for loading and saving data. Use the --async flag for potential performance sequential_runner.py:68
                             gains.                                                                                                                    
                             https://docs.kedro.org/en/stable/nodes_and_pipelines/run_a_pipeline.html#load-and-save-asynchronou                        
                             sly                                                                                                                       
                    INFO     Attempting download for landing.bgee.homo_sapiens_expressions_advanced (CSVDataset) using http          origin_hooks.py:63
                             provider.                                                                                                                 
[05/28/25 11:28:40] INFO     Loading data from landing.bgee.homo_sapiens_expressions_advanced (CSVDataset)...                 kedro_data_catalog.py:757
[05/28/25 11:28:47] INFO     Running node: bgee: process_bgee() ->                                                                          node.py:367
[05/28/25 11:28:49] INFO     Saving data to bronze.bgee.gene_expressions_in_anatomy (CSVDataset)...                           kedro_data_catalog.py:715
[05/28/25 11:28:50] INFO     Completed node: bronze.bgee                                                                                  runner.py:244
                    INFO     Completed 1 out of 34 tasks                                                                                  runner.py:245
```

## How to work with notebooks

> Note: Using `make jupyterlab` to run your notebook provides these variables in scope: `catalog`, `context`, `pipelines` and `session`.
>
> JupyterLab is already included in the project requirements by default, so once you have run `uv sync` you will not need to take any extra steps before you use them.

### JupyterLab

You can start JupyterLab with:

```console
$ make jupyterlab

[02/01/25 00:22:01] INFO     Using 'conf/logging.yml' as logging configuration. You can change this by setting the KEDRO_LOGGING_CONFIG environment variable accordingly.                     __init__.py:270
/home/viti/Documents/repos/harvard/optimuskg/.venv/bin/python -m jupyter lab --MultiKernelManager.default_kernel_name=kedro_optimuskg
[I 2025-02-01 00:22:04.096 ServerApp] jupyter_lsp | extension was successfully linked.
[I 2025-02-01 00:22:04.101 ServerApp] jupyter_server_terminals | extension was successfully linked.
[I 2025-02-01 00:22:04.106 ServerApp] jupyterlab | extension was successfully linked.
[I 2025-02-01 00:22:04.111 ServerApp] notebook | extension was successfully linked.
[I 2025-02-01 00:22:04.361 ServerApp] notebook_shim | extension was successfully linked.
[I 2025-02-01 00:22:04.405 ServerApp] notebook_shim | extension was successfully loaded.
[I 2025-02-01 00:22:04.409 ServerApp] jupyter_lsp | extension was successfully loaded.
[I 2025-02-01 00:22:04.410 ServerApp] jupyter_server_terminals | extension was successfully loaded.
[I 2025-02-01 00:22:04.412 LabApp] JupyterLab extension loaded from /home/viti/Documents/repos/harvard/optimuskg/.venv/lib/python3.12/site-packages/jupyterlab
[I 2025-02-01 00:22:04.412 LabApp] JupyterLab application directory is /home/viti/Documents/repos/harvard/optimuskg/.venv/share/jupyter/lab
[I 2025-02-01 00:22:04.412 LabApp] Extension Manager is 'pypi'.
[I 2025-02-01 00:22:04.536 ServerApp] jupyterlab | extension was successfully loaded.
[I 2025-02-01 00:22:04.540 ServerApp] notebook | extension was successfully loaded.
[I 2025-02-01 00:22:04.541 ServerApp] Serving notebooks from local directory: /home/viti/Documents/repos/harvard/optimuskg
[I 2025-02-01 00:22:04.541 ServerApp] Jupyter Server 2.15.0 is running at:
[I 2025-02-01 00:22:04.541 ServerApp] http://localhost:8888/lab?token=3398bd1a7d0991e2079181a5c037e4d9cc757f37247eba27
[I 2025-02-01 00:22:04.541 ServerApp]     http://127.0.0.1:8888/lab?token=3398bd1a7d0991e2079181a5c037e4d9cc757f37247eba27
[I 2025-02-01 00:22:04.541 ServerApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 2025-02-01 00:22:04.609 ServerApp]

    To access the server, open this file in a browser:
        file:///home/viti/.local/share/jupyter/runtime/jpserver-14957-open.html
    Or copy and paste one of these URLs:
        http://localhost:8888/lab?token=3398bd1a7d0991e2079181a5c037e4d9cc757f37247eba27
        http://127.0.0.1:8888/lab?token=3398bd1a7d0991e2079181a5c037e4d9cc757f37247eba27
```

## Neo4j

This project uses Neo4j as its graph database. You can manage the Neo4j instance using the `Makefile`.

The typical workflow is:
1. Run the Kedro pipeline to generate the graph data.
2. Import the data into Neo4j.
3. Start the Neo4j container.
4. Interact with the database (e.g., export data).

### Import Data

After running the pipeline to generate the graph data, you can import it into Neo4j with:

```console
$ make neo4j-import-data
```

This command performs an offline import, creating the necessary database files. If a Neo4j container is already running, you may need to stop it before importing.

### Start Neo4j

To start the Neo4j container with the imported data, run:

```console
$ make neo4j
```

This will start a Neo4j container in the background. You can access the Neo4j Browser at `http://localhost:7474`.

### Export Data

You can export the entire database or the results of a specific query once the container is running.

To export the entire database to a JSONL file, run:

```console
$ make neo4j-export
```

The data will be saved in `data/export/optimuskg.jsonl`.

To export the results of a specific Cypher query, run:

```console
$ CYPHER_QUERY="MATCH (d:Disease) RETURN d" make neo4j-export
```

The results will be saved to a file in `data/export/` with a name derived from the query.

## Post-processing

After exporting the graph from Neo4j, you can perform additional processing steps.

### Convert to PG-JSONL

To convert the Neo4j JSONL export into a Property Graph (PG) compatible format, run:

```console
$ uv run cli neo4j-to-pg
```

This command reads the file from `data/export/optimuskg.jsonl` and writes the PG-JSONL version to `data/export/optimuskg.pg.jsonl`. You can specify different input and output paths using the `--in` and `--out` options.

### Knowledge Graph Metrics

To calculate metrics about the PG-JSONL graph, run:

```console
$ uv run cli write-metrics
```

This command reads the PG-JSONL file from `data/export/optimuskg.pg.jsonl` and saves the metrics to `data/export/metrics.json`. You can specify different input and output paths using the `--in` and `--out` options.

### Private Datasets

If you have access to private datasets, place them in the appropriate subdirectories under `data/landing/`. The pipeline will automatically use them if present.

If you do not have access, the Origin Hook will generate empty placeholder datasets in their place (see [line 52 in `optimuskg/hooks/origin/origin_hooks.py`](https://github.com/mims-harvard/optimuskg/blob/main/optimuskg/hooks/origin/origin_hooks.py#L52)). This allows pipeline nodes that depend on both public and private data to run, even if the private data is missing. As a result, you can still execute the pipeline and work with the public portions of the data without interruption.