import os
from pathlib import Path

from datasets.plugins import OfflineDataset


def _get_dataset_path(dataset: OfflineDataset) -> str:
    path = Path(dataset._executor.datastore_path) / "datastore"

    zodiac_service = os.environ.get("ZODIAC_SERVICE", None)
    if zodiac_service:
        path /= zodiac_service

    return str(path / dataset.program_name / dataset.name)
