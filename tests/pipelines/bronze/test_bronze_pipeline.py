import logging
from pathlib import Path

from tests.conftest import KedroSettings

logger = logging.getLogger(__name__)


def _replace_dataset_filepath(dataset):
    if hasattr(dataset, "_filepath"):
        # Check if the filepath is a local path or a URL/s3 endpoint
        original_path = Path(dataset._filepath)
        logger.info(f"Original path: {original_path}")
        test_path = Path(str(original_path).replace("data/", "tests/stub_data/"))
        logger.info(f"Test path: {test_path}")
        dataset._filepath = str(test_path)
        logger.info(dataset)
        return test_path


def test_bronze_pipeline(kedro: KedroSettings, caplog):
    # Update filepaths to point to test data
    for dataset in kedro.context.catalog.values():  # type: ignore[attr-defined]
        _ = _replace_dataset_filepath(dataset)
        break

    kedro.session.run(node_names=["bronze.bgee"])
    assert kedro.SUCCESSFUL_RUN_MSG in caplog.text
