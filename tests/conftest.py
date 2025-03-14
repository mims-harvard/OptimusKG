from collections.abc import Generator
from pathlib import Path

import pytest
from kedro.framework.context import KedroContext
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from pydantic import BaseModel, ConfigDict


class KedroSettings(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    session: KedroSession
    context: KedroContext
    SUCCESSFUL_RUN_MSG: str = "Pipeline execution completed successfully."


@pytest.fixture
def kedro() -> Generator[KedroSettings, None, None]:
    bootstrap_project(Path.cwd())
    with KedroSession.create(project_path=Path.cwd(), env="test") as session:
        yield KedroSettings(session=session, context=session.load_context())
