from pathlib import Path

import pytest
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

SUCCESSFUL_RUN_MSG = "Pipeline execution completed successfully."


@pytest.fixture
def session():
    # TODO: Set a stub_data/ directory to ensure that the data is present in different contexts like CI/CD.
    bootstrap_project(Path.cwd())
    with KedroSession.create(project_path=Path.cwd()) as session:
        yield session


@pytest.fixture
def context(session):
    return session.load_context()
