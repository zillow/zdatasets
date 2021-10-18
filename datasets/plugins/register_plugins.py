from datasets.dataset import Dataset
from datasets.plugins import (
    MetaflowExecutor,
    OfflineDataset,
    OfflineFlowDataset,
)


def register():
    from importlib_metadata import entry_points
    for entry in entry_points(group='datasets.plugins'):
        entry.load()

    entry_points(group='datasets.executors')

    # Register plugins
    Dataset.register_plugin(OfflineDataset, constructor_keys={"name"})
    Dataset.register_plugin(OfflineFlowDataset, constructor_keys={"flow_dataset"})

    # Register executors
    Dataset.register_executor(executor=MetaflowExecutor())
