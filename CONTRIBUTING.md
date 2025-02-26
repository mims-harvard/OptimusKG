# Contributing to OptimusKG

## Setting up the development environment

We use [`uv`](https://github.com/astral-sh/uv) and [`docker`](https://www.docker.com/) for our local development.

1. Install `uv` by following the instructions on the [uv website](https://docs.astral.sh/uv/getting-started/installation/).
2. Install `docker` by following the instructions on the [docker website](https://docs.docker.com/engine/install/).
3. Run the following command to install all dependencies and set up the development environment:

    ```console
    $ uv sync --all-extras

    Resolved 218 packages in 3ms
    Audited 215 packages in 0.28ms
    ```

## Pre-commit hooks

We use [`pre-commit`](https://pre-commit.com/) to run our pre-commit hooks. You can install the pre-commit hooks with:

```console
$ uv tool run pre-commit install
pre-commit installed at .git/hooks/pre-commit
```

## Running development scripts

We use [`hatch`](https://hatch.pypa.io/latest/) to run our development scripts. You can see all the available scripts with:

```console
uv tool run hatch run list
```

To run a specific script, use the following syntax:

```console
uv tool run hatch run <script-name>
```

The hatch scripts are defined in the `pyproject.toml` file.

## Running tests

We use [`pytest`](https://docs.pytest.org/en/stable/) to run our tests. You can run all the tests with:

```console
uv tool run pytest
```

