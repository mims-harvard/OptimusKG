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

```

### Running hatch scripts

We use [hatchling] for our script manager. You can see all the available scripts with `uv tool run hatch run CHANGE-THIS`.


## How to work with notebooks

> Note: Using `uv tool run kedro jupyter lab` to run your notebook provides these variables in scope: `catalog`, `context`, `pipelines` and `session`.
>
> JupyterLab is already included in the project requirements by default, so once you have run `uv sync` you will not need to take any extra steps before you use them.

### JupyterLab

You can start JupyterLab with:

```console
$ uv tool run kedro jupyter lab


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
