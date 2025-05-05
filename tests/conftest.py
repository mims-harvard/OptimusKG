from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

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
    with KedroSession.create(
        project_path=Path.cwd(), env="test", conf_source="tests/conf"
    ) as session:
        with patch("optimuskg.utils.KedroSession") as MockKedroSession:
            # Mock the KedroSession.create method to return the test session instead of the default one
            MockKedroSession.create.return_value.__enter__.return_value = session
            yield KedroSettings(session=session, context=session.load_context())
