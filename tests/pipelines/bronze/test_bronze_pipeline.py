import logging

from tests.conftest import KedroSettings

logger = logging.getLogger(__name__)


def test_bronze_nodes(kedro: KedroSettings, caplog):
    kedro.session.run(
        node_names=[
            "bronze.bgee",
            "bronze.ctd",
            "bronze.reactome_ncbi",
            "bronze.reactome_pathways",
            "bronze.gene_names",
        ]
    )

    assert kedro.SUCCESSFUL_RUN_MSG in caplog.text
