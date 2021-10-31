import os
from pathlib import Path

from datasets.plugins import BatchDatasetPlugin


def _get_batch_dataset_path(dataset: BatchDatasetPlugin) -> str:
    path = Path(dataset._executor.datastore_path) / "datastore"

    zodiac_service = os.environ.get("ZODIAC_SERVICE", None)
    if zodiac_service:
        path /= zodiac_service

    return str(
        path
        / (dataset.program_name if dataset.program_name else dataset._executor.current_program_name)
        / dataset.name
    )
