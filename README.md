# OptimusKG

[![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)](https://github.com/mims-harvard/optimuskg)
[![Release](https://github.com/mims-harvard/optimuskg/actions/workflows/cd.yml/badge.svg)](https://github.com/mims-harvard/optimuskg)

## Installation

Prerequisites for this project are:

- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- [docker](https://docs.docker.com/engine/install/)

Install dependencies with:

```console
$ uv sync --all-extras

Resolved 218 packages in 3ms
Audited 215 packages in 0.28ms
```

## Run it

### Download landing files

In order to run the project, you need to download the landing files. You can do this with the following command:

```console
uv run cli landing
```

There are some files that are not downloaded automatically, so you need to download them manually. Ask the team for the files and put them in the `data/landing` folder as follows:

```console
optimuskg/
└── data/
    └── landing/
        ├── bgee/
        │   └── Homo_sapiens_expr_advanced.tsv
        ├── ctd/
        │   └── CTD_exposure_events.csv
        ├── reactome/
        │   └── NCBI2Reactome.txt
        │   └── ReactomePathways.txt
        │   └── ReactomePathwaysRelation.txt
        ├── ncbigene/
        │   └── gene2go
        ├── drugbank/
        │   └── full database.xml
        │   └── drugbank_all_carrier_polypeptide_ids.csv
        │   └── drugbank_all_enzyme_polypeptide_ids.csv
        │   └── drugbank_all_target_polypeptide_ids.csv
        │   └── drugbank_all_transporter_polypeptide_ids.csv
        │   └── drugbank_vocabulary.csv
        ├── drugcentral/
        │   └── drugcentral-pgdump_20200918.sql
        └── opentargets/
            └── primekg_nodes.csv
            └── primekg_edges.csv
```

### Set up Neo4j volume permissions

Before running the project, ensure proper permissions are set for the Neo4j import volume. This allows both the Neo4j container and Kedro to write the BioCypher GraphML file. Run:

```console
$ sudo chmod -R 777 data/gold/neo4j && sudo chown -R $(id -u):$(id -g) data/gold/neo4j
```

### Run the project

```console
$ uv run kedro run
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

[02/01/25 00:20:45] INFO     Using 'conf/logging.yml' as logging configuration. You can change this by setting the KEDRO_LOGGING_CONFIG environment variable accordingly.                     __init__.py:270
                    INFO     Kedro project optimuskg                                                                                                                                           session.py:329
[02/01/25 00:20:47] INFO     Using synchronous mode for loading and saving data. Use the --async flag for potential performance gains.                                                sequential_runner.py:74
                             https://docs.kedro.org/en/stable/nodes_and_pipelines/run_a_pipeline.html#load-and-save-asynchronously
                    INFO     Loading data from landing.opentargets.drug_mappings (CSVDataset)...                                                                                          data_catalog.py:390
[02/01/25 00:20:50] INFO     Loading data from landing.opentargets.primekg_nodes (CSVDataset)...                                                                                          data_catalog.py:390
[02/01/25 00:20:51] INFO     Loading data from landing.opentargets.molecule (PartitionedDataset)...                                                                                       data_catalog.py:390
                    INFO     Running node: drug_mappings: process_drug_mappings([landing.opentargets.drug_mappings;landing.opentargets.primekg_nodes;landing.opentargets.molecule]) ->            node.py:367
                             [bronze.opentargets.drug_mappings]
[02/01/25 00:20:53] INFO     Saving data to bronze.opentargets.drug_mappings (CSVDataset)...                                                                                              data_catalog.py:432
                    INFO     Loading data from landing.opentargets.evidence.chembl (PartitionedDataset)...                                                                                data_catalog.py:390
                    INFO     Completed node: bronze.drug_mappings                                                                                                                               runner.py:250
                    INFO     Completed 1 out of 32 tasks                                                                                                                                        runner.py:251
                    INFO     Running node: chembl: process_chembl([landing.opentargets.evidence.chembl]) -> [bronze.opentargets.evidence.chembl]                                                  node.py:367
```

## How to work with notebooks

> Note: Using `uv run kedro jupyter lab` to run your notebook provides these variables in scope: `catalog`, `context`, `pipelines` and `session`.
>
> JupyterLab is already included in the project requirements by default, so once you have run `uv sync` you will not need to take any extra steps before you use them.

### JupyterLab

You can start JupyterLab with:

```console
$ uv run kedro jupyter lab

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

## Spin up Neo4j

```console
$ uv run cli neo4j

[02/26/25 02:37:40] INFO     Using 'conf/logging.yml' as logging configuration. You can change this by setting the KEDRO_LOGGING_CONFIG  __init__.py:270
                             environment variable accordingly.                                                                                          
                    INFO     Spinning up Neo4j service...                                                                                 __main__.py:28
                    INFO     Neo4j service started successfully.                                                                          __main__.py:33
                    INFO     Web interface (HTTP): http://localhost:7474                                                                  __main__.py:34
                    INFO     Web interface (HTTPS): https://localhost:7473  
```

