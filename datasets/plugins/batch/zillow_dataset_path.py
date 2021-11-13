import os
from pathlib import Path

from datasets.plugins import BatchDatasetPlugin


# TODO(talebz): AIP-5405 Create zillow-datasets library for Zillow Offline dataset policies
def _get_batch_dataset_path(dataset: BatchDatasetPlugin) -> str:
    path = Path(dataset._executor.datastore_path) / "datastore"

    zodiac_service = os.environ.get("ZODIAC_SERVICE", None)
    if zodiac_service:
        path /= zodiac_service

    return str(
        path
        / (dataset.program_name if dataset.program_name else dataset._executor.current_program_name)
        / dataset._table_name
    )
