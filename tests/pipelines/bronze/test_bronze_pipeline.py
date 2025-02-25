import logging
from pathlib import Path

from tests.conftest import KedroSettings

logger = logging.getLogger(__name__)


def test_bronze_pipeline(kedro: KedroSettings, caplog):
    # Update catalog entries to point to test data
    for name, dataset in kedro.context.catalog.items():  # type: ignore[attr-defined]
        if hasattr(dataset, "_filepath"):
            # NOTE: Here we are assuming that the dataset filepath is always a local path.
            original_path = Path(dataset._filepath)
            new_path = str(original_path).replace("data/", "tests/stub_data/")

            # Create new dataset with updated filepath
            dataset_type = type(dataset)
            dataset_args = {
                "filepath": new_path,
                "load_args": dataset._load_args
                if hasattr(dataset, "_load_args")
                else None,
                "fs_args": dataset._storage_options
                if hasattr(dataset, "_storage_options")
                else None,
            }

            # Add required arguments for SQLDumpQueryDataset
            if dataset_type.__name__ == "SQLDumpQueryDataset":
                dataset_args.update(
                    {
                        "query": dataset._describe()["query"],
                        "db_name": dataset._describe()["db_name"],
                    }
                )

            # Create new dataset with updated filepath
            new_dataset = dataset_type(**dataset_args)

            # Update catalog
            kedro.context.catalog[name] = new_dataset  # type: ignore[index]

    kedro.session.run(
        node_names=[
            "bronze.bgee",
            "bronze.ctd",
            "bronze.gene2go",
            "bronze.reactome_ncbi",
            "bronze.reactome_pathways",
            "bronze.gene_names",
        ]
    )
    assert kedro.SUCCESSFUL_RUN_MSG in caplog.text
