# OptimusKG

## Installation

Prerequisites for this project are:

- [uv](https://github.com/astral-sh/uv)
- [docker](https://docs.docker.com/engine/install/)

Install dependencies with:

```console
$ uv sync --all-extras

Resolved 218 packages in 3ms
Audited 215 packages in 0.28ms
```

## Run it

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

### Running hatch scripts

We use [hatch](https://hatch.pypa.io/latest/) as our project manager. You can see all the available scripts with:

```console
$ uv tool run hatch run list
bandit
clean
download_landing_files
interrogate
list
mypy
...
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

### How to ignore notebook output cells in `git`

To automatically strip out all output cell contents before committing to `git`, you can use tools like [`nbstripout`](https://github.com/kynan/nbstripout). For example, you can add a hook in `.git/config` with `nbstripout --install`. This will run `nbstripout` before anything is committed to `git`.

> _Note:_ Your output cells will be retained locally.


## How to test the project

Have a look at the file `src/tests/test_run.py` for instructions on how to write your tests. You can run your tests as follows:

```
uv tool run pytest
```

## Install pre-commit hooks

To install the pre-commit hooks run:

```bash
uv tool run pre-commit install
```
