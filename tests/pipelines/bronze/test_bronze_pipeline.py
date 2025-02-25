import logging
from pathlib import Path

from tests.conftest import KedroSettings

logger = logging.getLogger(__name__)


def test_bronze_pipeline(kedro: KedroSettings, caplog):
    # Update filepaths to point to test data
    for dataset in kedro.context.catalog.values():  # type: ignore[attr-defined]
        if hasattr(dataset, "_filepath"):
            # NOTE: Here we are assuming that the dataset filepath is always a local path.
            original_path = Path(dataset._filepath)
            test_path = Path(str(original_path).replace("data/", "tests/stub_data/"))
            dataset._filepath = str(test_path)

    kedro.session.run(
        node_names=[
            "bronze.bgee",
            "bronze.ctd",
            "bronze.gene2go",
            "bronze.reactome",
            "bronze.gene_names",
        ]
    )
    assert kedro.SUCCESSFUL_RUN_MSG in caplog.text
